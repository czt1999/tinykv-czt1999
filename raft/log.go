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
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hs, _, err := storage.InitialState()
	if err != nil {
		panic(err.Error())
	}
	fi, _ := storage.FirstIndex()
	li, _ := storage.LastIndex()
	l := &RaftLog{
		storage:         storage,
		committed:       hs.Commit,
		applied:         0,
		stabled:         li,
		entries:         make([]pb.Entry, 0),
		pendingSnapshot: nil,
	}
	ents, err := storage.Entries(fi, li+1)
	//log.Debugf("NewLog storage Entries %v", ents)
	for _, ent := range ents {
		l.entries = append(l.entries, ent)
	}
	// @TODO
	//sn, err := storage.Snapshot()
	//for err == ErrSnapshotTemporarilyUnavailable {
	//	time.Sleep(10 * time.Millisecond)
	//	sn, err = storage.Snapshot()
	//}
	//if err == nil {
	//	sn.GetData()
	//}
	return l
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) > 0 {
		si := l.physicalIndex(l.stabled)
		return l.entries[si+1:]
	}
	return []pb.Entry{}
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if len(l.entries) > 0 {
		ai := l.physicalIndex(l.applied)
		ci := l.physicalIndex(l.committed)
		return l.entries[ai+1 : ci+1]
	}
	return []pb.Entry{}
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	return 0
}

// LastTerm return the last term of the log entries
func (l *RaftLog) LastTerm() uint64 {
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Term
	}
	return 0
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	i = l.physicalIndex(i)
	if i < 0 || i >= uint64(len(l.entries)) {
		return 0, fmt.Errorf("illegal log index %v (physical)", i)
	}
	return l.entries[i].Term, nil
}

func (l *RaftLog) EntsAfter(i uint64) (ents []pb.Entry) {
	i = l.physicalIndex(i)
	if i < uint64(len(l.entries)) {
		return l.entries[i:]
	}
	return []pb.Entry{}
}

func (l *RaftLog) DropEnts(i uint64) {
	if i <= l.committed {
		panic("can not drop committed entries")
	}
	i = l.physicalIndex(i)
	if i < uint64(len(l.entries)) {
		l.entries = l.entries[:i]
		l.stabled = min(l.stabled, uint64(len(l.entries)))
	}
}

func (l *RaftLog) physicalIndex(i uint64) uint64 {
	return i - 1
}
