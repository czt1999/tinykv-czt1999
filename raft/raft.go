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
	// Your Code Here (2A).
	r := &Raft{
		id:   c.ID,
		Term: 0,
		Vote: 0,
		RaftLog: &RaftLog{
			storage:         c.Storage,
			committed:       0,
			applied:         c.Applied,
			stabled:         0,
			entries:         nil,
			pendingSnapshot: nil,
		},
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
	for _, peer := range c.peers {
		r.Prs[peer] = &Progress{}
	}
	return r
}

func (r *Raft) bcastAppend() {

}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	//msg := pb.Message{
	//    MsgType:  pb.MessageType_MsgAppend,
	//    To:       to,
	//    From:     r.id,
	//    Term:     r.Term,
	//    LogTerm:  0,
	//    Index:    0,
	//    Entries:  nil,
	//    Commit:   r.RaftLog.committed,
	//    Snapshot: nil,
	//    Reject:   false,
	//}
	//
	//r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	log.Debugf("%v sendHeartbeat %v", r.id, to)
	r.msgs = append(r.msgs, r.newMessage(pb.MessageType_MsgHeartbeat, to, false))
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
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
	// Your Code Here (2A).
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
	r.electionElapsed = -(r.electionTimeout + rand.Intn(r.electionTimeout))
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	log.Debugf("%v becomeCandidate", r.id)
	r.State = StateCandidate
	r.Lead = None
	r.Term += 1
	r.Vote = r.id
	r.votes[r.id] = true
	r.electionElapsed = -(r.electionTimeout + rand.Intn(r.electionTimeout))
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	log.Debugf("%v Step %v", r.id, m.MsgType.String())
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
		r.becomeCandidate()
		for peer := range r.Prs {
			if peer != r.id {
				r.msgs = append(r.msgs, r.newMessage(pb.MessageType_MsgRequestVote, peer, true))
				log.Debugf("%v MsgHup send RequestVote to %v", r.id, peer)
			}
		}
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		log.Debugf("%v RequestVote (From: %v)", r.id, m.From)
		mm := r.newMessage(pb.MessageType_MsgRequestVoteResponse, m.From, true)
		if m.Term < r.Term || (m.Term == r.Term && r.Vote != None && r.Vote != m.From) {
			mm.Reject = true
		} else if m.Commit < r.RaftLog.committed {
			mm.Reject = true
		} else {
			r.Term = m.Term
			r.Vote = m.From
		}
		r.msgs = append(r.msgs, mm)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		for peer := range r.Prs {
			if peer != r.id {
				r.msgs = append(r.msgs, r.newMessage(pb.MessageType_MsgRequestVote, peer, true))
				log.Debugf("%v MsgHup send RequestVote to %v", r.id, peer)
			}
		}
		log.Debugf("%v MsgHup send RequestVote over", r.id)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		log.Debugf("%v RequestVote (From: %v)", r.id, m.From)
		mm := r.newMessage(pb.MessageType_MsgRequestVoteResponse, m.From, true)
		if m.Term <= r.Term {
			mm.Reject = true
		} else {
			r.becomeFollower(m.Term, None)
			if m.Commit < r.RaftLog.committed {
				mm.Reject = true
			} else {
				r.Term = m.Term
				r.Vote = m.From
			}
		}
		r.msgs = append(r.msgs, mm)
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
		log.Debugf("%v MsgRequestVoteResponse (From: %v, Reject: %v, vcnt: %v)", r.id, m.From, m.Reject, vcnt)
		// more than majority (quorum), becomes leader
		if vcnt > len(r.Prs)/2 {
			r.becomeLeader()
		} else if len(r.votes)-vcnt > len(r.Prs)/2 {
			r.becomeFollower(r.Term, None)
		}
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		for peer := range r.Prs {
			if peer != r.id {
				r.sendHeartbeat(peer)
			}
		}
		r.heartbeatElapsed = -r.heartbeatTimeout
	case pb.MessageType_MsgRequestVote:
		log.Debugf("%v RequestVote (From: %v)", r.id, m.From)
		mm := r.newMessage(pb.MessageType_MsgRequestVoteResponse, m.From, true)
		if m.Term <= r.Term {
			mm.Reject = true
		} else {
			r.becomeFollower(m.Term, None)
			if m.Commit < r.RaftLog.committed {
				mm.Reject = true
			} else {
				r.Term = m.Term
				r.Vote = m.From
			}
		}
		r.msgs = append(r.msgs, mm)
	case pb.MessageType_MsgPropose:
		for _, ent := range m.Entries {
			r.RaftLog.entries = append(r.RaftLog.entries, *ent)
		}
	case pb.MessageType_MsgAppendResponse, pb.MessageType_MsgHeartbeatResponse,
		pb.MessageType_MsgHeartbeat, pb.MessageType_MsgAppend:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		}
	}
	return nil
}

func (r *Raft) newMessage(mt pb.MessageType, to uint64, withLogTermAndIndex bool) pb.Message {
	m := pb.Message{
		MsgType: mt,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	if withLogTermAndIndex {
		logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
		m.LogTerm = logTerm
		m.Index = r.RaftLog.LastIndex()
	}
	return m
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.Lead = m.From
	r.Term = m.Term
	r.electionElapsed = -(r.electionTimeout + rand.Intn(r.electionTimeout))
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	log.Debugf("%v handleHearbeat (Lead: %v, From: %v, Term: %v, m.Term: %v)", r.id, r.Lead, m.From, r.Term, m.Term)
	if m.Term < r.Term {
		return
	}
	switch r.State {
	case StateCandidate:
		// message's term is higher than candidate's
		// the candidate reverts to follower and updates its committed index
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
			r.RaftLog.committed = m.Commit
		}
	case StateFollower:
		//	message's term is higher than follower's
		//	the follower updates its leaderID with the ID from the message
		if m.Term >= r.Term {
			r.Lead = m.From
			r.RaftLog.committed = m.Commit
			r.electionElapsed = -(r.electionTimeout + rand.Intn(r.electionTimeout))
		}
	}
	r.msgs = append(r.msgs, r.newMessage(pb.MessageType_MsgHeartbeatResponse, r.Lead, false))
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
