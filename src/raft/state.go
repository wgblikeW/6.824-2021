package raft

import (
	"sync"
	"sync/atomic"
)

// RaftState captures the state of a Raft node: Follower, Candidate, Leader,
type RaftState uint32

const (
	// Follower is the initial state of a Raft node.
	Follower RaftState = iota

	// Candidate is one of the valid states of a Raft node
	Candidate

	// Leader is one of the valid states of a Raft node.
	Leader
)

type raftState struct {
	// The current term, cache of StableStore
	CurrentTerm uint64

	// Highest committed log entry
	CommitIndex uint64

	// Last applied log to the FSM
	LastApplied uint64

	// The current state
	State RaftState

	// protects 4 next fields
	lastLock sync.Mutex

	// Cache the latest snapshot index/term
	lastSnapshotIndex uint64
	lastSnapshotTerm  uint64

	// Cache the latest log from LogStore
	LastLogIndex uint64
	LastLogTerm  uint64
}

func (rs *raftState) getState() RaftState {
	stateAddr := (*uint32)(&rs.State)
	return RaftState(atomic.LoadUint32(stateAddr))
}

func (r *raftState) setState(s RaftState) {
	stateAddr := (*uint32)(&r.State)
	atomic.StoreUint32(stateAddr, uint32(s))
}

func (r *raftState) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&r.CurrentTerm)
}

func (r *raftState) setCurrentTerm(term uint64) {
	atomic.StoreUint64(&r.CurrentTerm, term)
}

func (r *raftState) getLastLog() (index, term uint64) {
	r.lastLock.Lock()
	defer r.lastLock.Unlock()
	index = r.LastLogIndex
	term = r.LastLogTerm
	return
}

func (r *raftState) setLastLog(index, term uint64) {
	r.lastLock.Lock()
	defer r.lastLock.Unlock()
	r.LastLogIndex = index
	r.LastLogTerm = term
}

func (r *raftState) getCommitIndex() uint64 {
	return atomic.LoadUint64(&r.CommitIndex)
}

func (r *raftState) setCommitIndex(index uint64) {
	atomic.StoreUint64(&r.CommitIndex, index)
}
func (r *raftState) getLastApplied() uint64 {
	return atomic.LoadUint64(&r.LastApplied)
}

func (r *raftState) setLastApplied(index uint64) {
	atomic.StoreUint64(&r.LastApplied, index)
}

// getLastIndex returns the last index in stable storage.
// Either from the last log or from the last snapshot.
func (r *raftState) getLastIndex() uint64 {
	r.lastLock.Lock()
	defer r.lastLock.Unlock()
	i := r.LastLogIndex
	return i
}

// getLastEntry returns the last index and term in stable storage.
// Either from the last log or from the last snapshot.
func (r *raftState) getLastEntry() (uint64, uint64) {
	r.lastLock.Lock()
	defer r.lastLock.Unlock()
	return r.LastLogIndex, r.LastLogTerm
}
