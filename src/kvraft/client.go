package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderID int64 // Clerk should remember the LeaderID(maybe) in order to improve the performance
	nextSeq  int64
	clientID int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderID = 0
	ck.nextSeq = nrand()
	ck.clientID = nrand()
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) replyHandler(Err Err, Op string) string {
	leaderID := ck.getLeaderID()
	switch Err {
	case OK:
		return OK
	case ErrWrongLeader:
		DPrintf("[Client] failed to call %v %v Reason: %v", leaderID, Op, Err)
		ck.setLeaderID((leaderID + 1) % int64(len(ck.servers))) // Optimistically, maybe the next is the leader
		return ErrWrongLeader
	default:
		DPrintf("[Client] failed to call %v %v Reason: %v", leaderID, Op, Err)
		return ErrNoKey
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	args := &GetArgs{
		Key:      key,
		ClientID: ck.getClientID(),
	}

	// Get ops return should reach majority in order to prevent out-dated data
	for {
		leaderID := ck.getLeaderID()
		server := ck.servers[leaderID]
		reply := &GetReply{}
		ok := server.Call("KVServer.Get", args, reply)
		if !ok {
			DPrintf("[Client] failed to call %v Due to remote Server Crash Or Network partition", leaderID)
			ck.setLeaderID((leaderID + 1) % int64(len(ck.servers))) // Optimistically, maybe the next is the leader
		} else {
			// Get the remote server's reply
			if n := ck.replyHandler(reply.Err, "Get"); n == OK || n == ErrNoKey {
				DPrintf("[Client] succeed to call %v Key %v Value %v", leaderID, args.Key, reply.Value)
				return reply.Value // return "" when ErrNoKey present
			}
		}
		<-time.Tick(time.Millisecond * 200)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	DPrintf("[Client %v] Key %v Value %v Seq %v", ck.getClientID(), key, value, ck.getNextSeq())
	// setting RPCs params for request and reply
	args := &PutAppendArgs{
		ClientID: ck.getClientID(),
		Key:      key,
		Value:    value,
		Op:       op,
		Seq:      ck.getNextSeq(),
	}
	reply := &PutAppendReply{}

	// keep trying for Ops until it has been successed
	for {
		leaderID := ck.getLeaderID()
		server := ck.servers[leaderID]
		ok := server.Call("KVServer.PutAppend", args, reply)
		if !ok {
			// Cannot send RPCs to remote Server
			DPrintf("[Client] failed to call %v", leaderID)
			ck.setLeaderID((leaderID + 1) % int64(len(ck.servers))) // Optimistically, maybe the next is the leader
		} else { // RPCs call success and get reply
			if ck.replyHandler(reply.Err, args.Op) == OK {
				ck.setNextSeq(reply.ExpectedSeq)
				break
			}
		}
		<-time.Tick(time.Millisecond * 200)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
