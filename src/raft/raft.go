package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
	"github.com/hashicorp/go-hclog"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

// constant value used for vote
const (
	NONE int32 = -1
)

type InstallSnapshotArgs struct {
	Term             uint64 // leader's term
	LeaderID         int32  // so follower can redirect clients
	LastIncluedIndex uint64 // the snapshot replaces all entries up through and including this index
	LastIncludeTerm  uint64 // term of lastIncluedIndex
	Data             []byte // raw bytes of the snapshot chunck
}

type InstallSnapshotReply struct {
	Term    uint64 // currentTerm, for leader to update itslef
	Success bool   // indicates the installSnapshotRPC perform or not
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Candidates arise an election and they need to provide
	// Information to follower to decide whether vote to one
	// Candidate based on some specific rules.

	Term         uint64 // candidate's term
	CandidateID  int32  // identity of subject who becomes a candidate
	LastLogIndex uint64 // Index of candidate's last log entry
	LastLogTerm  uint64 // Term of candidate's last log entry
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoteID      int32
	Term        uint64 // currentTerm, for candidate to update itself
	VoteGranted bool   // true means candidate received vote
	Reason      string
}

//
// Invoked by leader to replicate log entries; also used as heartbeat
// Sending to ---> replicated server (follower)
//
type AppendEntriesArgs struct {
	Term         uint64     // leader's term
	LeaderID     int32      // so follower can redirect clients
	PrevLogIndex uint64     // index of log entry immediately preceding new ones
	PrevLogTerm  uint64     // term of preLogIndex entry
	Entries      []logEntry // log entries to store(empty for heartbeat; may send more than one for efficiency)
	LeaderCommit uint64     // leader's commitIndex
}

//
// Reply to ---> leader server
// reply to AppendEntries
//
type AppendEntriesReply struct {
	Term        uint64 // currentTerm, for leader to update itself
	Success     bool   // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIdx uint64 // The term of the conflicting entry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool = false
	// Your code here (2A).
	term = int(rf.getCurrentTerm())
	if rf.getState() == Leader {
		isleader = true
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.getSerRaftState())
}

func (rf *Raft) getSerRaftState() []byte {
	bytesWriter := new(bytes.Buffer)
	e := labgob.NewEncoder(bytesWriter)

	// Raft States need to be persistent
	e.Encode(rf.getCurrentTerm())
	e.Encode(rf.getLastVote())
	e.Encode(rf.getCommitIndex())
	e.Encode(rf.getLastApplied())
	lastIdx, lastTerm := rf.getLastEntry()
	e.Encode(lastIdx)
	e.Encode(lastTerm)
	e.Encode(rf.getRangeEntreis(rf.getLastSnapshotIndex(), lastIdx+1))
	e.Encode(rf.getLastSnapshotIndex())
	e.Encode(rf.getLastSnapshotTerm())
	data := bytesWriter.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.logger.Debug("server", rf.getServerID(), "readPersist")
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock() // lock for log
	bytesReader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(bytesReader)
	var currentTerm, commitIdx, lastApplied, lastLogIdx, lastLogTerm, lastSnapshotIdx, lastSnapshotTerm uint64
	var votedFor int32
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&commitIdx) != nil ||
		decoder.Decode(&lastApplied) != nil ||
		decoder.Decode(&lastLogIdx) != nil ||
		decoder.Decode(&lastLogTerm) != nil ||
		decoder.Decode(&rf.log) != nil ||
		decoder.Decode(&lastSnapshotIdx) != nil ||
		decoder.Decode(&lastSnapshotTerm) != nil {
		rf.logger.Error("error in unserialize the raft state")
	}
	rf.setCurrentTerm(currentTerm)
	rf.setVoteFor(votedFor)
	rf.setCommitIndex(commitIdx)
	rf.setLastApplied(lastApplied)
	rf.setLastLogIdx(lastLogIdx)
	rf.setLastLogTerm(lastLogTerm)
	rf.setLastSnapshotIndex(lastSnapshotIdx)
	rf.setLastSnapshotTerm(lastSnapshotTerm)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	if index > int(rf.getLastSnapshotIndex()) {
		// client snapshot index is larger than ours
		lastIdx := rf.getLastIndex()
		// trimmedEntries includes the logEntry with index of [lastIdx]
		trimmedEntries := rf.getRangeEntreis(uint64(index), lastIdx+1)

		rf.mu.Lock()
		rf.log = append([]logEntry{}, trimmedEntries...)
		lastSnapshotTerm := rf.log[0].Term
		lastEntry := rf.log[len(rf.log)-1]
		rf.mu.Unlock()

		rf.setLastSnapshotIndex(uint64(index))
		rf.setLastSnapshotTerm(lastSnapshotTerm)

		if lastEntry.Index > rf.getLastSnapshotIndex() {
			rf.setLastLogIdx(lastEntry.Index)
			rf.setLastSnapshotTerm(lastEntry.Term)
		} else {
			rf.setLastLogIdx(rf.getLastSnapshotIndex())
			rf.setLastLogTerm(rf.getLastSnapshotTerm())
		}
		rf.setLastApplied(Max(rf.getLastApplied(), rf.getLastSnapshotIndex()))
		rf.setCommitIndex(Max(rf.getCommitIndex(), rf.getLastSnapshotIndex()))

		// persist the raftState and Snapshot, we can not call the persist()
		rf.persister.SaveStateAndSnapshot(rf.getSerRaftState(), snapshot)
	}
}

func (rf *Raft) canCommit() bool {
	rf.mat_and_next_locker.Lock()
	defer rf.mat_and_next_locker.Unlock()
	majority, count := len(rf.peers)/2+1, 1
	for i := 0; i < len(rf.peers); i++ {
		if rf.matchIndex[i] == rf.getLastIndex() {
			count++
		}
	}
	rf.logger.Info("canCommit count ", count, "leader server ", rf.getServerID())
	return count >= majority
}

func (rf *Raft) apply() {
	for {
		select {
		case <-rf.notifyApplyCh:
			applyEntries := rf.getRangeEntreis(rf.getLastApplied()+1, rf.getCommitIndex()+1)
			rf.logger.Info("Server", rf.getServerID(), "apply entries ", applyEntries)
			rf.setLastApplied(rf.getCommitIndex())
			rf.persist()
			for _, entry := range applyEntries {
				rf.applyCh <- ApplyMsg{CommandValid: true, CommandIndex: int(entry.Index), CommandTerm: int(entry.Term), Command: entry.Command}
			}
		}
	}
}

// Reinitialized volatile state on leaders after election
func (rf *Raft) initIndex() {
	rf.mat_and_next_locker.Lock()
	defer rf.mat_and_next_locker.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastIndex() + 1
		rf.matchIndex[i] = 0
	}
}

//
// arise RequestVote RPCs calls
//
func (rf *Raft) RequestVote(electionTimeout <-chan time.Time) {
	rf.logger.Info("issues request vote", rf.getServerID())
	args := &RequestVoteArgs{
		Term:        rf.getCurrentTerm(),
		CandidateID: rf.getServerID(),
	}
	lastLogIdx, lastLogTerm := rf.getLastEntry()
	args.LastLogIndex, args.LastLogTerm = lastLogIdx, lastLogTerm
	voteCh := make(chan struct{}, len(rf.peers))
	var goroutineTracer sync.WaitGroup
	goroutineTracer.Add(1)
	rf.setVoteFor(rf.getServerID()) // candidate vote for itself
	rf.persist()
	go func() {
		// vote counter
		counter := 1 // contains candidate its vote
		voteNeeds := len(rf.peers)/2 + 1
		for {
			select {
			case <-electionTimeout:
				rf.logger.Warn("election timeout happens, restart election", rf.getServerID())
				goroutineTracer.Done()
				return
			case <-voteCh:
				counter++
				if counter >= voteNeeds && rf.getState() == Candidate {
					// candidate become leader
					rf.logger.Info("candidate become leader", rf.getServerID())
					rf.setState(Leader)
					rf.initIndex()
					// leader initialization
					goroutineTracer.Done()
					return
				}
			}
		}
	}()

	// RPCs call to other peers
	for peerIdx, peer := range rf.peers {
		if peerIdx == int(rf.getServerID()) {
			// don't call itself
			continue
		}

		go func(peerIdx int, peer *labrpc.ClientEnd, args *RequestVoteArgs) {
			reply := &RequestVoteReply{}
			ok := peer.Call("Raft.RequestVoteHandler", args, reply)
			if !ok {
				rf.logger.Error("calling remote server fails", peerIdx)
			}
			if reply.VoteGranted {
				voteCh <- struct{}{} // vote for candidate
				rf.logger.Info("receive vote from", peerIdx)
			} else {
				rf.logger.Warn("candidate", rf.getServerID(), "do not receive vote from", peerIdx, "reason", reply.Reason)
				if reply.Term > rf.getCurrentTerm() {
					rf.setState(Follower)
					rf.setCurrentTerm(reply.Term)
					rf.persist()
				}
			}
		}(peerIdx, peer, args)
	}
	goroutineTracer.Wait()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.startLocker.Lock()
	defer rf.startLocker.Unlock()
	if rf.getState() != Leader {
		return -1, -1, false
	}
	lastIdx, _ := rf.getLastEntry()
	currentTerm := rf.getCurrentTerm()
	entry := logEntry{Index: lastIdx + 1, Term: currentTerm, Command: command}
	rf.setLastLogIdx(lastIdx + 1)
	rf.setLastLogTerm(currentTerm)
	rf.mu.Lock()
	rf.log = append(rf.log, entry)
	rf.logger.Info("Client AppendEntries", entry, "LeaderID", rf.getServerID())
	rf.mu.Unlock()
	rf.persist()
	return int(lastIdx) + 1, int(currentTerm), true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func randomTimeout(minVal time.Duration, maxVal time.Duration) time.Duration {
	// minVal reasonably should be at least less equal than maxVal
	return time.Duration(rand.Int63n(int64(maxVal)-int64(minVal))) + minVal
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) runRaft() {

	for !rf.killed() {
		switch rf.getState() {
		case Follower:
			rf.runFollower()
		case Leader:
			rf.runLeader()
		case Candidate:
			rf.runCandidate()
		}
	}
}

func (rf *Raft) runFollower() {
	rf.logger.Info("entering follower state", rf.getServerID())
	rf.heartBeatTimer.Reset(randomTimeout(300*time.Millisecond, 800*time.Millisecond))
	for rf.getState() == Follower {
		<-rf.heartBeatTimer.C
		// when election timeout transition state to Candidate
		rf.logger.Warn("heartbeat timeout, transistion state to candidate", rf.getServerID())
		rf.setState(Candidate)
	}
}

func (rf *Raft) runCandidate() {
	rf.logger.Info("entering candidate state", rf.getServerID())

	for rf.getState() == Candidate {
		// New election Term, increase Term
		rf.setCurrentTerm(rf.getCurrentTerm() + 1)
		rf.persist()
		electionTimer := time.NewTimer(randomTimeout(300*time.Millisecond, 800*time.Millisecond))
		rf.RequestVote(electionTimer.C)
	}
}

func (rf *Raft) runLeader() {
	rf.logger.Info("entering leader state", rf.getServerID())

	for rf.getState() == Leader {
		// replicate log
		heartBeatTimer := time.NewTimer(time.Millisecond * 30)
		<-heartBeatTimer.C
		rf.logReplicate()
	}
}

//
// installSnapshot Send installSnapshotRPC to remote peer who
// is far behind the leader
// TODO: Unfininshed yet
//
func (rf *Raft) installSnapshot(peerIdx int) {
	// only leader can send installSnapshot
	if rf.getState() != Leader {
		return
	}
	args := &InstallSnapshotArgs{
		Term:             rf.getCurrentTerm(),
		LeaderID:         rf.getServerID(),
		LastIncluedIndex: rf.getLastSnapshotIndex(),
		LastIncludeTerm:  rf.getLastSnapshotTerm(),
		Data:             rf.persister.ReadSnapshot(),
	}
	reply := &InstallSnapshotReply{}

	ok := rf.peers[peerIdx].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		rf.logger.Error("fail to send InstallSnapshotRPC to", peerIdx)
	} else {
		if reply.Success {
			// remote server performs installsnapshot successfully
			// update the nextIndex and matchIndex
			rf.mat_and_next_locker.Lock()
			rf.nextIndex[peerIdx] = Max(args.LastIncluedIndex+1, rf.nextIndex[peerIdx])
			rf.matchIndex[peerIdx] = Max(args.LastIncluedIndex, rf.matchIndex[peerIdx])
			rf.mat_and_next_locker.Unlock()
		} else {
			// check whether we should give up leadership
			if reply.Term > rf.getCurrentTerm() {
				rf.setState(Follower)
				rf.setCurrentTerm(reply.Term)
				rf.setVoteFor(NONE)
			}
		}
	}
}

func (rf *Raft) logReplicate() {
	if rf.getState() != Leader {
		return
	}
	for peerIdx, peer := range rf.peers {
		if peerIdx == int(rf.getServerID()) {
			// skip itself
			continue
		}
		rf.mat_and_next_locker.Lock()
		if rf.nextIndex[peerIdx] <= rf.getLastSnapshotIndex() {
			go rf.installSnapshot(peerIdx)
			rf.mat_and_next_locker.Unlock()
			continue
		}
		rf.mat_and_next_locker.Unlock()
		go func(peer *labrpc.ClientEnd, peerIdx int) {
			args := &AppendEntriesArgs{
				LeaderCommit: rf.getCommitIndex(),
				Term:         rf.getCurrentTerm(),
				LeaderID:     rf.getServerID(),
				Entries:      nil,
			}
			rf.mat_and_next_locker.Lock()
			prevLogIdx := rf.nextIndex[peerIdx] - 1
			rf.mat_and_next_locker.Unlock()
			var storeEntry logEntry
			err := rf.getEntry(prevLogIdx, &storeEntry)
			if err != nil {
				rf.logger.Error(err.Error())
			}
			prevLogTerm := storeEntry.Term
			args.PrevLogIndex = prevLogIdx
			args.PrevLogTerm = prevLogTerm
			lastLogIdx, _ := rf.getLastEntry()
			reply := &AppendEntriesReply{}

			rf.mat_and_next_locker.Lock()
			if rf.nextIndex[peerIdx] < lastLogIdx+1 {
				args.Entries = rf.getRangeEntreis(rf.nextIndex[peerIdx], lastLogIdx+1)
				// rf.logger.Info("sending entries ", args.Entries, "to", peerIdx)
			}
			rf.mat_and_next_locker.Unlock()
			// rf.logger.Info("sending appendentries rpc call to", peerIdx)
			ok := peer.Call("Raft.AppendEntriesHandler", args, reply)
			if !ok {
				rf.logger.Error("sending append entries rpc call fails", rf.getServerID())
			} else {
				if !reply.Success {
					if reply.Term > rf.getCurrentTerm() {
						rf.setCurrentTerm(reply.Term)
						rf.setState(Follower)
						rf.setVoteFor(NONE)
						rf.persist()
						return
					} else {
						rf.mat_and_next_locker.Lock()
						rf.nextIndex[peerIdx] = Max(1, Min(reply.ConflictIdx, rf.getLastIndex()+1))
						rf.mat_and_next_locker.Unlock()
					}
				} else { // reply Success is true
					// rf.logger.Info("reply Success from server", peerIdx)
					prevLogIdx, entriesLen := args.PrevLogIndex, len(args.Entries)
					if prevLogIdx+uint64(entriesLen) >= rf.nextIndex[peerIdx] {
						rf.mat_and_next_locker.Lock()
						rf.nextIndex[peerIdx] = rf.nextIndex[peerIdx] + uint64(entriesLen)
						rf.matchIndex[peerIdx] = rf.nextIndex[peerIdx] - 1
						rf.mat_and_next_locker.Unlock()
					}
				}
				if rf.canCommit() {
					rf.setCommitIndex(rf.getLastIndex())
					rf.persist()
					rf.notifyApplyCh <- struct{}{}
				}
			}
		}(peer, peerIdx)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// settings of logger
	LogLevel := "DEBUG"
	// LogOutput := os.Stderr
	LogOutput, _ := os.OpenFile("./raft.log", os.O_APPEND|os.O_WRONLY, 0600)

	// Raft Server initialization
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             int32(me),
		heartBeatTimer: time.NewTimer(10 * time.Second),
		// persistent state on all servers
		votedFor: NONE,
		log:      []logEntry{{0, 0, nil}}, // log entry at index 0 is unused
		// volaile state on all servers
		applyCh:       applyCh,
		notifyApplyCh: make(chan struct{}),
		// volaile state on leaders
		nextIndex:  make([]uint64, len(peers)),
		matchIndex: make([]uint64, len(peers)),
		logIndex:   1,
		logger: hclog.New(&hclog.LoggerOptions{
			Name:   "raft",
			Level:  hclog.LevelFromString(LogLevel),
			Output: LogOutput,
		}),
		raftState: raftState{
			CurrentTerm:       0,
			CommitIndex:       0,
			LastApplied:       0,
			LastLogIndex:      0,
			LastLogTerm:       0,
			lastSnapshotIndex: 0,
			lastSnapshotTerm:  0,
		},
	}
	rf.setState(Follower)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.runRaft()
	go rf.apply()
	return rf
}
