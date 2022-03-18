package raft

import "time"

//
// invoked by the AppendEntries RPCs
// AppendEntries was only sent by leader, receiver can be follower, candidate and
// outdated leader
//
func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.heartBeatTimer.Reset(randomTimeout(300*time.Millisecond, 800*time.Millisecond))

	reply.Success = false
	reply.Term = rf.getCurrentTerm()
	reply.ConflictIdx = 0

	if rf.getCurrentTerm() > args.Term {
		return
	}

	if args.Term > rf.getCurrentTerm() {
		rf.setCurrentTerm(args.Term)
		rf.setVoteFor(NONE)
		rf.setState(Follower)
		rf.persist()
	}

	// check consistency with leader
	lastidx, lastTerm := rf.getLastEntry()
	var prevTerm uint64
	if lastidx == args.PrevLogIndex {
		prevTerm = lastTerm
	} else {
		var prevEntry logEntry
		if err := rf.getEntry(args.PrevLogIndex, &prevEntry); err != nil {
			rf.logger.Warn("failed to get previous log",
				"previous-indx", args.PrevLogIndex,
				"last-index", lastidx,
				"error", err)
			reply.ConflictIdx = lastidx
			return
		}
		prevTerm = prevEntry.Term
	}

	if prevTerm != args.PrevLogTerm {
		rf.logger.Warn("previous log term mis-match",
			"ours", prevTerm,
			"remote", args.PrevLogTerm)
		targetTerm := Min(prevTerm, args.PrevLogTerm)
		var firstEntryOfTerm logEntry
		rf.getTermFirstEntry(targetTerm, &firstEntryOfTerm)
		reply.ConflictIdx = firstEntryOfTerm.Index
		return
	}

	reply.Success = true
	// Process any new entries
	var newEntries []logEntry
	if len(args.Entries) > 0 {
		lastIdx, _ := rf.getLastEntry()
		for i, entry := range args.Entries {
			if entry.Index > lastIdx {
				newEntries = args.Entries[i:]
				break
			}
			var storeEntry logEntry
			if err := rf.getEntry(entry.Index, &storeEntry); err != nil {
				rf.logger.Warn("failed to get log entry",
					"index", entry.Index,
					"error", err)
				return
			}
			if entry.Term != storeEntry.Term {
				rf.logger.Warn("clearing log suffix",
					"from", entry.Index,
					"to", lastIdx, "server", rf.getServerID())
				rf.logger.Debug("before delete entries", rf.getRangeEntreis(rf.getLastSnapshotIndex(), rf.getLastIndex()+1))
				if err := rf.deleteRange(entry.Index, lastIdx); err != nil { // delete any conflict entries
					rf.logger.Error("failed to clear log suffix", "error", err)
					return
				}
				rf.logger.Debug("log entries after delete", rf.getRangeEntreis(rf.getLastSnapshotIndex(), rf.getLastIndex()+1))
				newEntries = args.Entries[i:]
				break
			}
		}
	}
	rf.logger.Debug("Server", rf.getServerID(), "newEntries ", args.Entries, "leaderTerm", args.Term)
	if n := len(newEntries); n > 0 {
		rf.storeLogs(&newEntries)
		last := newEntries[n-1]
		rf.setLastLog(last.Index, last.Term)
		rf.persist()
		// rf.logger.Info("Follower", rf.getServerID(), "LogEntries", rf.getRangeEntreis(0, rf.getLastIndex()+1))
	}
	rf.heartBeatTimer.Reset(randomTimeout(300*time.Millisecond, 800*time.Millisecond))
	// Update the commit index
	if args.LeaderCommit > 0 && args.LeaderCommit > rf.getCommitIndex() {
		idx := Min(args.LeaderCommit, rf.getLastIndex())
		rf.setCommitIndex(idx)
		rf.persist()
		rf.notifyApplyCh <- struct{}{}
	}
	rf.heartBeatTimer.Reset(randomTimeout(300*time.Millisecond, 800*time.Millisecond))
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.heartBeatTimer.Reset(randomTimeout(300*time.Millisecond, 800*time.Millisecond))

	if args.Term == rf.getCurrentTerm() && rf.getLastVote() == args.CandidateID {
		// network duplicate
		reply.VoteGranted, reply.Term = true, rf.getCurrentTerm()
		return
	}

	if rf.getCurrentTerm() > args.Term || // invalid candidate
		(rf.getCurrentTerm() == args.Term && rf.getLastVote() != NONE) { // the server has voted in this term
		reply.Term, reply.VoteGranted = rf.getCurrentTerm(), false
		reply.Reason = "remote server term higher than candidate or it vote to other server in this term"
		return
	}

	if args.Term > rf.getCurrentTerm() {
		rf.setCurrentTerm(args.Term)
		rf.setVoteFor(NONE)
		rf.persist()
		if rf.getState() != Follower {
			rf.setState(Follower)
		}
	}

	reply.Term = rf.getCurrentTerm()
	lastLogIdx, lastLogTerm := rf.getLastEntry()

	if lastLogTerm > args.LastLogTerm || // log with the later term is more up-to-date
		(lastLogTerm == args.LastLogTerm && lastLogIdx > args.LastLogIndex) { // logs end with the same term, longer log is more up-to-date
		reply.VoteGranted = false
		reply.Reason = "remote server has more up-to-date log"
		return
	}
	reply.VoteGranted = true
	rf.setVoteFor(args.CandidateID)
	rf.persist()
	rf.heartBeatTimer.Reset(randomTimeout(300*time.Millisecond, 800*time.Millisecond))
}
