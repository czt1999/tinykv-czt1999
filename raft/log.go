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
	"github.com/pingcap-incubator/tinykv/log"
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
	first uint64

	termBeforeFirst uint64

	tag uint64
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
		storage:   storage,
		committed: hs.Commit,
		applied:   fi - 1,
		stabled:   li,
		first:     fi,
		entries:   make([]pb.Entry, 0),
	}
	ents, err := storage.Entries(fi, li+1)
	log.Debugf("NewLog storage FirstIndex %v LastIndex %v", fi, li)
	if err != nil {
		panic(err.Error())
	}
	l.termBeforeFirst, _ = storage.Term(fi - 1)
	l.entries = append(l.entries, ents...)
	return l
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	fi, _ := l.storage.FirstIndex()
	if fi > l.first {
		t, _ := l.storage.Term(fi - 1)
		l.compact(fi, t)
	}
}

func (l *RaftLog) compact(newFirst uint64, termBefore uint64) {
	if newFirst > l.first {
		// compacted entries
		ents := make([]pb.Entry, 0, len(l.entries))
		if newFirst-l.first < uint64(len(l.entries)) {
			ents = append(ents, l.entries[newFirst-l.first:]...)
		}
		l.first = newFirst
		l.termBeforeFirst = termBefore
		l.stabled = max(l.stabled, newFirst-1)
		l.committed = max(l.committed, newFirst-1)
		l.applied = max(l.applied, newFirst-1)
		l.entries = ents
		//log.Warnf("[%v] compacted %v", l.tag, fi - 1)
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) > 0 {
		si := l.physicalIndex(l.stabled + 1)
		if si > l.stabled {
			log.Panicf("stabled %v, first %v", l.stabled, l.first)
		}
		if si > uint64(len(l.entries)) {
			log.Panicf("stabled %v, first %v, len(entries) %v, LastIndex %v", l.stabled, l.first, len(l.entries), l.LastIndex())
		}
		return l.entries[si:]
	}
	return []pb.Entry{}
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if len(l.entries) > 0 {
		ai := l.physicalIndex(l.applied + 1)
		ci := l.physicalIndex(l.committed + 1)
		if ai > l.applied {
			log.Warnf("invalid applied %v first %v comitted %v", l.applied, l.first, l.committed)
		}
		return l.entries[ai:ci]
	}
	return []pb.Entry{}
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	return l.first - 1
}

// LastTerm return the last term of the log entries
func (l *RaftLog) LastTerm() uint64 {
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Term
	}
	return l.termBeforeFirst
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if i < l.first {
		return l.termBeforeFirst, nil
	}
	i = l.physicalIndex(i)
	if i >= uint64(len(l.entries)) {
		return 0, fmt.Errorf("illegal log index %v/%v (physical)", i, len(l.entries))
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

func (l *RaftLog) dropEnts(i uint64) {
	if i <= l.committed {
		log.Panicf("can not drop committed entries %v %v", i, l.committed)
	}
	i = l.physicalIndex(i)
	if i < uint64(len(l.entries)) {
		l.entries = l.entries[:i]
		l.stabled = min(l.stabled, l.LastIndex())
	}
}

func (l *RaftLog) physicalIndex(i uint64) uint64 {
	return i - l.first
}
