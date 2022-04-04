package kvraft

import (
	"sync/atomic"
)

func (ck *Clerk) getClientID() int64 {
	return atomic.LoadInt64(&ck.clientID)
}

func (ck *Clerk) getNextSeq() int64 {
	return atomic.LoadInt64(&ck.nextSeq)
}

func (ck *Clerk) setNextSeq(seq int64) {
	atomic.StoreInt64(&ck.nextSeq, seq)
}

func (kv *KVServer) getExpectedSeqUClientID(clientID int64) (int64, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if seq, exists := kv.expectedNextSeq[clientID]; exists {
		return seq, true
	} else {
		return -1, false
	}
}

func (kv *KVServer) setExpectedSeqUClientID(clientID int64, expectedSeq int64) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.expectedNextSeq[clientID] = expectedSeq
}

func (kv *KVServer) setupNotifyCh(logIdx int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.expectedLogEntryMap[logIdx] = make(chan NotifyApplyMsg)
}

func (ck *Clerk) getLeaderID() int64 {
	return atomic.LoadInt64(&ck.leaderID)
}

func (ck *Clerk) setLeaderID(idx int64) {
	atomic.StoreInt64(&ck.leaderID, idx)
}

func (kv *KVServer) getStorageValue(key string) (string, Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, exists := kv.Storage[key]; !exists {
		DPrintf("[Server %v] getStorageValue Key %v Value %v", kv.me, key, value)
		return "", ErrNoKey
	} else {
		DPrintf("[Server %v] getStorageValue Key %v Value %v", kv.me, key, value)
		return value, OK
	}
}

func (kv *KVServer) doPut(key string, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.Storage[key] = value
}

func (kv *KVServer) doAppend(key string, value string) Err {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if oldV, exists := kv.Storage[key]; exists {
		kv.Storage[key] = oldV + value
	} else {
		kv.Storage[key] = value
	}
	return OK
}
