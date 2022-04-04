package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientID int64  // ClientID used to identify the duplicate request
	Seq      int64
}

type PutAppendReply struct {
	Err         Err
	ExpectedSeq int64
}

type GetArgs struct {
	Key      string
	ClientID int64 // ClientID used to identify the duplicate request
}

type GetReply struct {
	Err   Err
	Value string
}

type NotifyApplyMsg struct {
	LogIdx  uint64
	LogTerm uint64
	Value   string
	Err     Err
}
