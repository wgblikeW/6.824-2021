package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	Debug  = true
	PUT    = "Put"
	APPEND = "Append"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	persister           *raft.Persister // where K/V Service keep persistence of its state
	maxraftstate        int             // snapshot if log grows this big
	Storage             map[string]string
	expectedLogEntryMap map[int]chan NotifyApplyMsg // tells apply() where to send its notification
	notifySnapshot      chan int
	expectedNextSeq     map[int64]int64
}

// Get handles the RPCs call from Clerk and do Get Ops
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Start raft consensus algorithm to make replicates across the cluster
	logIdx, logTerm, isLeader := kv.rf.Start(
		GetArgs{Key: args.Key})
	// Only Leader can do operations and get consensus across the cluster (no optimization)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[Server %v] Receiving Get RPCs", kv.me)

	kv.setupNotifyCh(logIdx)
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.expectedLogEntryMap, logIdx)
	}()
	/**
		await for the Raft peers reach consensus
		the leader maybe out-dated or disconnect from the cluster
		during processing the AppendEntries RPCs thus It downgrades to follower
		Reciving other ApplyMsg which comes from another leader, and it's not what we
		are expectint. We should confirm the logIdx, logTerm here before we mess up
		If the server downgrade when it is waiting for the client, it should notify the client
		with ErrWrongLeader
	**/
	kv.mu.Lock()
	clientCh := kv.expectedLogEntryMap[logIdx]
	kv.mu.Unlock()
	reply.Value = ""
	select {
	case <-time.Tick(time.Second * 3):
		// timeout for waiting Raft apply Ops
		DPrintf("[Server %v] Timeout For waiting Raft peers to reach Consensus", kv.me)
		reply.Err = ErrWrongLeader
	case resp := <-clientCh:
		DPrintf("[Server %v] Raft Peers reach Consensus For Op %v %v", kv.me, "Get", resp)
		if resp.LogTerm != uint64(logTerm) {
			// leadership has been transfered during log replicate
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = resp.Err
		DPrintf("[Server %v] Key %v reply.Value %v", kv.me, args.Key, resp.Value)
		reply.Value = resp.Value
	}
}

// PutAppend handles the RPCs call from Clerk and do Put Or Append Ops
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Start raft consensus algorithm to make replicates across the cluster
	logIdx, logTerm, isLeader := kv.rf.Start(
		PutAppendArgs{Key: args.Key, Value: args.Value, Op: args.Op, Seq: args.Seq, ClientID: args.ClientID}) // not use the Refer
	// Only Leader can do operations and get consensus across the cluster (no optimization)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[Server %v] Receiving PutAppend RPCs", kv.me)
	// setup notifyCh for client
	kv.setupNotifyCh(logIdx)
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.expectedLogEntryMap, logIdx)
	}()
	/**
		await for the Raft peers reach consensus
		the leader maybe out-dated or disconnect from the cluster
		during processing the AppendEntries RPCs thus It downgrades to follower
		Reciving other ApplyMsg which comes from another leader, and it's not what we
		are expectint. We should confirm the logIdx, logTerm here before we mess up
	**/
	kv.mu.Lock()
	clientCh := kv.expectedLogEntryMap[logIdx]
	kv.mu.Unlock()
	select {
	case <-time.Tick(time.Second * 3):
		// timeout for waiting Raft apply Ops
		reply.Err = ErrWrongLeader
		DPrintf("[Server %v] Timeout For waiting Raft peers to reach Consensus", kv.me)
	case resp := <-clientCh:
		DPrintf("[Server %v] Raft Peers reach Consensus For Op %v %v", kv.me, "Put/Append", resp.Err)
		if resp.LogTerm != uint64(logTerm) {
			// leadership has been transfered during log replicate
			reply.Err = ErrWrongLeader
			return
		}
		reply.ExpectedSeq = args.Seq + 1
		reply.Err = resp.Err
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// doGet implement the Get Ops to the K/V Server
func (kv *KVServer) doGet(_getArgs *GetArgs, respToClient *NotifyApplyMsg) {
	defer func() {
		// notify the Client who is waiting for the Ops to be applied
		// (maybe such Client doesn't exist)
		kv.mu.Lock()
		if expectedClientCh, exists := kv.expectedLogEntryMap[int(respToClient.LogIdx)]; exists {
			expectedClientCh <- *respToClient
		}
		kv.mu.Unlock()
	}()
	value, err := kv.getStorageValue(_getArgs.Key)
	kv.notifySnapshot <- int(respToClient.LogIdx)
	respToClient.Err = err
	respToClient.Value = value
}

// doPutAppend implement the Put/Append Ops to the K/V Server
func (kv *KVServer) doPutAppend(_putAppendArgs *PutAppendArgs, respToClient *NotifyApplyMsg) {
	DPrintf("[Server %v] doPutAppend %v", kv.me, _putAppendArgs)
	defer func() {
		// K/V Server State has been updated, notifying the waiting client
		kv.mu.Lock()
		if expectedClientCh, exists := kv.expectedLogEntryMap[int(respToClient.LogIdx)]; exists {
			expectedClientCh <- *respToClient
		}
		kv.mu.Unlock()
	}()
	expectedSeq, exists := kv.getExpectedSeqUClientID(_putAppendArgs.ClientID)
	if exists && _putAppendArgs.Seq < expectedSeq {
		// client duplicate the Ops
		respToClient.Err = OK
		respToClient.Value = ""
		return
	}

	switch _putAppendArgs.Op {
	case PUT:
		kv.doPut(_putAppendArgs.Key, _putAppendArgs.Value)
		respToClient.Value = ""
		respToClient.Err = OK
	case APPEND:
		err := kv.doAppend(_putAppendArgs.Key, _putAppendArgs.Value)
		respToClient.Value = ""
		respToClient.Err = err
	}
	kv.setExpectedSeqUClientID(_putAppendArgs.ClientID, _putAppendArgs.Seq+1)
	kv.notifySnapshot <- int(respToClient.LogIdx)
}

// apply do validation of the command and apply command to K/V Server
func (kv *KVServer) apply(msg raft.ApplyMsg) {
	if msg.CommandValid {
		finalRespToClient := NotifyApplyMsg{LogIdx: uint64(msg.CommandIndex), LogTerm: uint64(msg.CommandTerm)}
		// valid FSM Command apply to FSM
		if getOps, ok := msg.Command.(GetArgs); ok {
			// Receive an Get Command
			kv.doGet(&getOps, &finalRespToClient)
		}
		if putAppendOps, ok := msg.Command.(PutAppendArgs); ok {
			// Receive an PutAppend Command
			kv.doPutAppend(&putAppendOps, &finalRespToClient)
		}
	}
}

func (kv *KVServer) disableClientConn() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.expectedLogEntryMap = make(map[int]chan NotifyApplyMsg, 1000)
}

func (kv *KVServer) watcher() {
	for {
		select {
		case applyMsg := <-kv.applyCh:
			// Receiving applyMsg new logs were committed
			DPrintf("[Server %v] Receiving ApplyMsg From ApplyCh %v", kv.me, applyMsg)
			if applyMsg.CommandValid != true && applyMsg.Command.(string) == "LeaderAlter" {
				kv.disableClientConn()
			}
			if applyMsg.SnapshotValid == true {
				// installRPC Receive
				DPrintf("[Server %v] InstallSnapshotRPC receive", kv.me)
				kv.readSnapshot(applyMsg.Snapshot)
			}
			kv.apply(applyMsg)
		}
	}
}

func (kv *KVServer) readSnapshot(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if data == nil || len(data) < 1 {
		return
	}

	bytesReader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(bytesReader)
	if decoder.Decode(&kv.expectedNextSeq) != nil ||
		decoder.Decode(&kv.Storage) != nil {
		DPrintf("[Server %v] error when reading snapshot", kv.me)
	}
}

func (kv *KVServer) snapshot(lastIncludedIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	bytesWriter := new(bytes.Buffer)
	e := labgob.NewEncoder(bytesWriter)
	e.Encode(&kv.expectedNextSeq)
	e.Encode(&kv.Storage)
	kv.rf.Snapshot(lastIncludedIndex, bytesWriter.Bytes())
}

// periodically check whether need to take a snapshot when new Log Entry
// was appened to the log
func (kv *KVServer) stateCamera() {
	for {
		select {
		case commandIdx := <-kv.notifySnapshot:
			if kv.persister.RaftStateSize() > kv.maxraftstate && kv.maxraftstate != -1 {
				kv.snapshot(commandIdx)
			}
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})

	kv := new(KVServer)
	kv.Storage = make(map[string]string)
	kv.expectedNextSeq = make(map[int64]int64)
	kv.expectedLogEntryMap = make(map[int]chan NotifyApplyMsg)
	kv.notifySnapshot = make(chan int, 100)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.rf.BootstrapStateMachine()
	go kv.watcher()
	go kv.stateCamera()
	return kv
}
