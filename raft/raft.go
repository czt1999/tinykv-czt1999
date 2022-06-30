// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}
	r := &Raft{
		id:               c.ID,
		Term:             hs.Term,
		Vote:             hs.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: -c.HeartbeatTick,
		electionElapsed:  -(c.ElectionTick + rand.Intn(c.ElectionTick)),
		leadTransferee:   0,
		PendingConfIndex: 0,
	}
	r.RaftLog.applied = max(r.RaftLog.applied, c.Applied)
	if len(c.peers) > 0 {
		for _, peer := range c.peers {
			r.Prs[peer] = &Progress{Match: 0, Next: 1}
		}
	} else {
		for _, peer := range cs.Nodes {
			r.Prs[peer] = &Progress{Match: 0, Next: 1}
		}
	}
	r.RaftLog.tag = r.id
	return r
}

// HardState return the current hard state
func (r *Raft) HardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) bcastAppend() {
	r.debug("bcastAppend %v", r.RaftLog.entries)
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// should send snapshot
	if r.Prs[to].Next < r.RaftLog.first {
		return r.sendSnapshot(to)
	}
	m := r.newMessage(pb.MessageType_MsgAppend, to)
	// prev log index and term
	m.Index = r.Prs[to].Next - 1
	m.LogTerm, _ = r.RaftLog.Term(m.Index)
	if r.Prs[to].Next > r.RaftLog.LastIndex() {
		if r.Prs[to].Match+1 == r.Prs[to].Next {
			// empty Append to announce committed
			r.debug("sendAppend %v (announce committed %v)", to, m.Commit)
			r.msgs = append(r.msgs, m)
			return true
		} else {
			// uncaught
			r.debug("sendAppend %v (ignore)", to)
			return false
		}
	}
	ents := r.RaftLog.EntsAfter(r.Prs[to].Next)
	m.Entries = make([]*pb.Entry, len(ents))
	for i := range ents {
		m.Entries[i] = &pb.Entry{
			EntryType: ents[i].EntryType,
			Term:      ents[i].Term,
			Index:     ents[i].Index,
			Data:      ents[i].Data,
		}
		r.Prs[to].Next = ents[i].Index + 1
	}
	r.debug("sendAppend %v %v", to, len(m.Entries))
	r.msgs = append(r.msgs, m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	r.msgs = append(r.msgs, r.newMessage(pb.MessageType_MsgHeartbeat, to))
}

func (r *Raft) sendSnapshot(to uint64) bool {
	m := r.newMessage(pb.MessageType_MsgSnapshot, to)
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err == ErrSnapshotTemporarilyUnavailable {
		return false
	}
	if err != nil {
		log.Error(err)
		return false
	}
	log.Warnf("[%v] sendSnapshot %v Next %v first %v", r.id, to, r.Prs[to].Next, r.RaftLog.first)
	m.Snapshot = &snapshot
	r.msgs = append(r.msgs, m)
	r.Prs[to].Next = snapshot.Metadata.Index + 1
	return true
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower, StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
	}
}

func (r *Raft) tickElection() {
	if r.electionElapsed += 1; r.electionElapsed >= 0 {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}

func (r *Raft) tickHeartbeat() {
	if r.heartbeatElapsed += 1; r.heartbeatElapsed >= 0 {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
	r.Vote = None
	r.electionElapsed = -(r.electionTimeout + rand.Intn(r.electionTimeout))
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.debug("becomeCandidate")
	r.State = StateCandidate
	r.Lead = None
	r.Term += 1
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.electionElapsed = -(r.electionTimeout + rand.Intn(r.electionTimeout))
	if len(r.Prs) <= 1 {
		r.becomeLeader()
		return
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.debug("becomeLeader")
	r.State = StateLeader
	for _, pr := range r.Prs {
		pr.Match = 0
		pr.Next = r.RaftLog.LastIndex() + 1
	}
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{
			EntryType: pb.EntryType_EntryNormal,
		}},
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	//r.debug("Step %v", m.MsgType.String())
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleHup(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVoteWhenIsFollower(m)
	case pb.MessageType_MsgSnapshot:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
			r.handleSnapshot(m)
		}
	case pb.MessageType_MsgTimeoutNow:
		if m.From == r.Lead {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleHup(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVoteWhenIsNotFollower(m)
	case pb.MessageType_MsgRequestVoteResponse:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
			return nil
		}
		r.votes[m.From] = !m.Reject
		vcnt := 0
		for _, v := range r.votes {
			if v {
				vcnt += 1
			}
		}
		r.debug("MsgRequestVoteResponse (From: %v, Reject: %v, vcnt: %v)", m.From, m.Reject, vcnt)
		// more than majority (quorum), becomes leader
		if vcnt > len(r.Prs)/2 {
			r.becomeLeader()
		} else if len(r.votes)-vcnt > len(r.Prs)/2 {
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgSnapshot:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
			r.handleSnapshot(m)
		}
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.handleBeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVoteWhenIsNotFollower(m)
	case pb.MessageType_MsgPropose:
		for _, ent := range m.Entries {
			ent.Term = r.Term
			ent.Index = r.RaftLog.LastIndex() + 1
			r.RaftLog.entries = append(r.RaftLog.entries, *ent)
		}
		r.Prs[r.id].Match = r.RaftLog.LastIndex()
		r.Prs[r.id].Next = r.Prs[r.id].Match + 1
		if len(r.Prs) == 1 {
			r.RaftLog.committed = r.RaftLog.LastIndex()
		} else {
			r.bcastAppend()
		}
	case pb.MessageType_MsgAppendResponse:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
			return nil
		}
		if m.Reject {
			toMatch := m.Index
			for t, _ := r.RaftLog.Term(toMatch); t != m.LogTerm && toMatch >= r.RaftLog.first; toMatch -= 1 {
			}
			r.debug("adjust Next %v -> %v for %v", r.Prs[m.From].Next, toMatch+1, m.From)
			r.Prs[m.From].Match = toMatch
			r.Prs[m.From].Next = toMatch + 1
			r.sendAppend(m.From)
			return nil
		} else {
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = m.Index + 1
		}
		if m.Index <= r.RaftLog.committed {
			if m.Commit < r.RaftLog.committed {
				r.sendAppend(m.From)
			}
			return nil
		}
		// try to update committed
		newCommitted := r.RaftLog.committed
		for i := r.RaftLog.committed + 1; i <= r.RaftLog.LastIndex(); i++ {
			term, _ := r.RaftLog.Term(i)
			mcnt := 0
			for _, pr := range r.Prs {
				if pr.Match >= i {
					mcnt++
				}
			}
			if mcnt > len(r.Prs)/2 && term == r.Term {
				newCommitted = i
			}
		}
		if newCommitted > r.RaftLog.committed {
			r.debug("increment commited %v", newCommitted)
			r.RaftLog.committed = newCommitted
			r.bcastAppend()
		}
	case pb.MessageType_MsgHeartbeatResponse:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
			return nil
		}
		// send log to follower which doesn't have update-to-date log
		if m.Index < r.RaftLog.LastIndex() {
			r.Prs[m.From].Next = m.Index + 1
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgHeartbeat, pb.MessageType_MsgAppend:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		}
	}
	return nil
}

func (r *Raft) newMessage(mt pb.MessageType, to uint64) pb.Message {
	m := pb.Message{
		MsgType: mt,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	return m
}

func (r *Raft) newMessageWithLogTermAndIndex(mt pb.MessageType, to uint64) pb.Message {
	m := r.newMessage(mt, to)
	m.LogTerm = r.RaftLog.LastTerm()
	m.Index = r.RaftLog.LastIndex()
	return m
}

// handleHup handle MsgHup
func (r *Raft) handleHup(m pb.Message) {
	r.becomeCandidate()
	for peer := range r.Prs {
		if peer != r.id {
			r.msgs = append(r.msgs, r.newMessageWithLogTermAndIndex(pb.MessageType_MsgRequestVote, peer))
			r.debug("MsgHup send RequestVote to %v", peer)
		}
	}
}

// handleBeat handle MsgBeat
func (r *Raft) handleBeat(m pb.Message) {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
	r.heartbeatElapsed = -r.heartbeatTimeout
}

// handleRequestVoteWhenIsFollower handle RequestVote RPC request
func (r *Raft) handleRequestVoteWhenIsFollower(m pb.Message) {
	r.debug("RequestVote (From: %v, m.Term: %v, m.Index: %v, m.LogTerm: %v, r.Term: %v, r.Vote: %v)",
		m.From, m.Term, m.Index, m.LogTerm, r.Term, r.Vote)
	mr := r.newMessageWithLogTermAndIndex(pb.MessageType_MsgRequestVoteResponse, m.From)
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastIndex)
	if m.Term < r.Term || (m.Term == r.Term && r.Vote != None && r.Vote != m.From) {
		mr.Reject = true
	} else if m.LogTerm < lastLogTerm || (m.LogTerm == lastLogTerm && m.Index < lastIndex || m.Commit < r.RaftLog.committed) {
		mr.Reject = true
	} else {
		r.Term = m.Term
		r.Vote = m.From
		r.electionElapsed = -r.electionTimeout
	}
	r.msgs = append(r.msgs, mr)
	if m.Term > r.Term {
		r.Term = m.Term
		r.Lead = None
	}
}

func (r *Raft) handleRequestVoteWhenIsNotFollower(m pb.Message) {
	if m.Term <= r.Term {
		r.debug("RequestVote (From: %v, m.Term: %v, r.Term: %v, r.Vote: %v) Reject", m.From, m.Term, r.Term, r.Vote)
		mr := r.newMessageWithLogTermAndIndex(pb.MessageType_MsgRequestVoteResponse, m.From)
		mr.Reject = true
		r.msgs = append(r.msgs, mr)
	} else {
		r.debug("revert to follower with a higher term %v", m.Term)
		r.becomeFollower(m.Term, None)
		r.handleRequestVoteWhenIsFollower(m)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.Term < r.Term {
		r.msgs = append(r.msgs, r.newMessageWithLogTermAndIndex(pb.MessageType_MsgAppendResponse, m.From))
		return
	}
	// candidate should revert to follower because it indicates that there is a valid leader
	if r.State == StateCandidate {
		r.becomeFollower(m.Term, m.From)
	}
	r.Term = m.Term
	r.Lead = m.From
	r.electionElapsed = -(r.electionTimeout + rand.Intn(r.electionTimeout))
	// check safety to append entries
	if m.Index <= r.RaftLog.LastIndex() {
		prevTerm, _ := r.RaftLog.Term(m.Index)
		if prevTerm == m.LogTerm {
			// append
			lastIndex := m.Index
			for _, ent := range m.Entries {
				lastIndex = ent.Index
				if ent.Index <= r.RaftLog.LastIndex() {
					logTerm, _ := r.RaftLog.Term(ent.Index)
					if ent.Term == logTerm {
						continue
					} else {
						// drop conflicting entries
						r.debug("handleAppend drop ents with index >= %v", ent.Index)
						r.RaftLog.dropEnts(ent.Index)
					}
				}
				r.RaftLog.entries = append(r.RaftLog.entries, *ent)
			}
			// update committed
			r.RaftLog.committed = max(r.RaftLog.committed, min(lastIndex, m.Commit))
			r.debug("update committed %v", r.RaftLog.committed)
			r.msgs = append(r.msgs, r.newMessageWithLogTermAndIndex(pb.MessageType_MsgAppendResponse, m.From))
			return
		}
		r.debug("handleAppend invalid index/term (%v/%v) here (-/%v) first %v last (%v/%v) %v", m.Index, m.LogTerm, prevTerm, r.RaftLog.first, r.RaftLog.LastIndex(), r.RaftLog.LastTerm(), len(r.RaftLog.entries))
	}
	r.debug("handleAppend invalid index/term (%v/%v) here (%v/%v)", m.Index, m.LogTerm, r.RaftLog.LastIndex(), r.RaftLog.LastTerm())
	// tell the leader to decrease Next
	mr := r.newMessageWithLogTermAndIndex(pb.MessageType_MsgAppendResponse, m.From)
	mr.Reject = true
	r.msgs = append(r.msgs, mr)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	//r.debug("handleHearbeat (Lead: %v, From: %v, Term: %v, m.Term: %v, m.Commit: %v)",
	//	r.Lead, m.From, r.Term, m.Term, m.Commit)
	if m.Term >= r.Term {
		switch r.State {
		case StateCandidate:
			// message's term is higher than candidate's
			// the candidate reverts to follower
			if m.Term > r.Term {
				r.becomeFollower(m.Term, m.From)
			}
		case StateFollower:
			//	message's term is higher than follower's
			//	the follower updates its leaderID with the ID from the message
			r.Lead = m.From
			r.electionElapsed = -(r.electionTimeout + rand.Intn(r.electionTimeout))
		}
	}
	r.msgs = append(r.msgs, r.newMessageWithLogTermAndIndex(pb.MessageType_MsgHeartbeatResponse, m.From))
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	// restore the raft internal state like the term, commit index and
	// membership information, etc. from `eraftpb.SnapshotMetadata`
	metaData := m.Snapshot.Metadata
	r.debug("handleSnapshot term: %v, index: %v, nodes: %v", metaData.Term, metaData.Index, metaData.ConfState)
	if metaData.Index > r.RaftLog.LastIndex() {
		r.RaftLog.pendingSnapshot = m.Snapshot
		r.Prs = make(map[uint64]*Progress)
		for _, peer := range metaData.ConfState.Nodes {
			r.Prs[peer] = &Progress{Match: 0, Next: 1}
		}
		r.RaftLog.compact(metaData.Index+1, metaData.Term)
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) debug(s string, v ...interface{}) {
	log.Debugf("#%v [%v-%s] "+s, append([]interface{}{r.id, r.Term, r.State.String()[5:]}, v...)...)
}
