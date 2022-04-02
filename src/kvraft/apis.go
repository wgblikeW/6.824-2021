package kvraft

import "sync/atomic"

func (ck *Clerk) getLeaderID() int64 {
	return atomic.LoadInt64(&ck.leaderID)
}

func (ck *Clerk) setLeaderID(idx int64) {
	atomic.StoreInt64(&ck.leaderID, idx)
}

func (ck *Clerk) getClientID() int64 {
	return atomic.LoadInt64(&ck.clientID)
}

func (ck *Clerk) setClientID(clientID int64) {
	atomic.StoreInt64(&ck.clientID, clientID)
}

func (kv *KVServer) getStorageValue(key string) (string, Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, exists := kv.Storage[key]; !exists {
		return "", OK
	} else {
		return value, ErrNoKey
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
		return OK
	} else {
		return ErrNoKey
	}
}
