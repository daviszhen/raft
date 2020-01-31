package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	Duplicate      = "Duplicate"
	LogChanged     = "LogChanged"
	ClientsOnSameIndex = "ClientsOnSameIndex"
	Outdated       = "Outdated"
	RpcTimeout     = "RpcTimeout"
	UpdateSnapshot = "UpdateSnapshot"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client int
	ReqNumber int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Client int
	ReqNumber int
}

type GetReply struct {
	Err   Err
	Value string
}
