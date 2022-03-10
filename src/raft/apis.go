package raft

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"github.com/hashicorp/go-hclog"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int32               // this peer's index into peers[]
	dead      int32               // set by Kill()

	raftState
	heartBeatTimer *time.Timer
	logIndex       uint64
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh     chan ApplyMsg
	applyLocker sync.Mutex
	startLocker sync.Mutex
	// Persistent state on all servers
	votedFor int32      // candidatedId that received vote in current term (or null if none)
	log      []logEntry // log entries, each entry contains command for state machine, and term when entry was received by leader(first index is 1)

	// Volatile state on leaders
	nextIndex           []uint64 // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex          []uint64 // for each server, index of highest log entry known to be replicated on server
	mat_and_next_locker sync.Mutex
	logger              hclog.Logger
}

func (rf *Raft) storeLogs(logEntries *[]logEntry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = append(rf.log, *logEntries...)
}

func (rf *Raft) getEntry(index uint64, logEntry *logEntry) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index >= uint64(len(rf.log)) {
		return errors.New("log index out of range")
	}
	Entry := rf.log[index]
	*logEntry = Entry
	return nil
}

func (rf *Raft) getTermFirstEntry(term uint64, log *logEntry) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	_, lastTerm := rf.getLastEntry()
	rf.logger.Debug("target Term", term, "Last Term", lastTerm)
	var i uint64
	entriesLen := uint64(len(rf.log))
	for i = 0; i < entriesLen && rf.log[i].Term != term; i++ {
	}
	// no match term re-transmision all entries
	if entriesLen == i {
		*log = rf.log[0]
		return nil
	}
	*log = rf.log[i]
	return nil
}

func (rf *Raft) deleteRange(from uint64, to uint64) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if from > to {
		return errors.New("from larger than to")
	}
	rf.log = rf.log[0:from]
	lastEntry := rf.log[len(rf.log)-1]
	rf.setLastLog(lastEntry.Index, lastEntry.Term)
	return nil
}

func (rf *Raft) setVoteFor(candidateID int32) {
	atomic.StoreInt32(&rf.votedFor, candidateID)
}

func (rf *Raft) getLastVote() int32 {
	return atomic.LoadInt32(&rf.votedFor)
}

func (rf *Raft) quorumSize() int {
	return len(rf.peers)
}

func (rf *Raft) getServerID() int32 {
	return atomic.LoadInt32(&rf.me)
}

func (rf *Raft) getRangeEntreis(fromIdx uint64, toIdx uint64) []logEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log[fromIdx:toIdx]
}

func (rf *Raft) getSingleEntry(index uint64) logEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log[index]
}

func (rf *Raft) setLastLogIdx(logIdx uint64) {
	rf.lastLock.Lock()
	defer rf.lastLock.Unlock()
	rf.LastLogIndex = logIdx
}

func (rf *Raft) setLastLogTerm(logTerm uint64) {
	rf.lastLock.Lock()
	defer rf.lastLock.Unlock()
	rf.LastLogTerm = logTerm
}
