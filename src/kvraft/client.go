package kvraft

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "time"
//import "log"
//import "fmt"

var clerkNo int = 0
var clerkMutex sync.Mutex

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader    int
	reqNumber       int
	client                int
	clientMutex sync.Mutex
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
	// You'll have to add code here.
	clerkMutex.Lock()
	ck.client     = clerkNo
	clerkNo++
	clerkMutex.Unlock()
	ck.lastLeader = 0
	ck.reqNumber = 0
	return ck
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

	// You will have to modify this function.
	var reqNo int = 0
	ck.clientMutex.Lock()
	reqNo = ck.reqNumber
	ck.reqNumber++
	ck.clientMutex.Unlock()
	for true   {
		req := new(GetArgs)
		req.Key = key
		req.Client = ck.client
		req.ReqNumber = reqNo
		reply := new(GetReply)
		//log.Printf("Call GET")
		ok :=   ck.servers[ck.lastLeader].Call("KVServer.Get", req, reply)
		//log.Printf("Call GET return")
		if ok {
			if reply.Err == "OK" || reply.Err == "Duplicate" {
				return reply.Value
			}
		}else{
			//log.Printf("Get timeout")
		}
		
		ck.clientMutex.Lock()
		ck.lastLeader++
		ck.lastLeader %= len(ck.servers)
		ck.clientMutex.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	return ""
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
	// You will have to modify this function.
	var reqNo int = 0
	ck.clientMutex.Lock()
	reqNo = ck.reqNumber
	ck.reqNumber++
	ck.clientMutex.Unlock()
	for true{
		req := new(PutAppendArgs)
		req.Key = key 
		req.Value = value
		req.Op = op
		req.Client = ck.client
		req.ReqNumber = reqNo
		reply    := new(PutAppendReply)
		//log.Printf("Call PutAppend")
		ok :=ck.servers[ck.lastLeader].Call("KVServer.PutAppend", req, reply)
		//log.Printf("Call PutAppend return")
		if ok {
			if reply.Err == "OK" || reply.Err == "Duplicate" {
				return 
			}
		}else{
			//log.Printf("PutAppend timeout")
		}
		
		ck.clientMutex.Lock()
		ck.lastLeader++
		ck.lastLeader %= len(ck.servers)
		ck.clientMutex.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	return 
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
