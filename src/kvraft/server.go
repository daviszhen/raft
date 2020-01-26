package kvraft

import (
	"container/list"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	GET    = 0
	PUT    = 1
	APPEND = 2
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client    int
	ReqNumber int
	OpType    int
	Key       string
	Value     string
}

type LogGroup struct {
	index     int
	term      int
	cmd       Op
	pAArgs    *PutAppendArgs
	pAReply   *PutAppendReply
	getArgs   *GetArgs
	getReply  *GetReply
	done      bool
	mutex     sync.Mutex
	condition *sync.Cond
	createTime time.Time
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	wait          map[int]*list.List
	lastReqNumber map[int]int
	data          map[string]string
	//最新被应用到状态机的日志Index和Term
	lastAppliedIndex int
	lastAppliedTerm  int
	//time for RPC wait
	//值不能太短
	rpcTimeout     int 
	//raft持久化
	raftPersister *raft.Persister          // Object to hold this peer's persisted state
}

func (kv *KVServer) InitKVServer() {
	atomic.StoreInt32(&kv.dead, 0)
	kv.wait = make(map[int]*list.List)
	kv.lastReqNumber = make(map[int]int)
	kv.data	= make(map[string]string)
	//1000,3000，4000不能过 ，5000大部分能过，10000能过
	kv.rpcTimeout = 10000
	go kv.ApplyStateMachineRoutine()
	go kv.RpcTimeoutTimer()
}

    
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// You r cod e here.
	cmd := Op{args.Client,args.ReqNumber,GET,args.Key,""}
	//DPrintf("Get call raft")
	index,term,isLeader := kv.rf.Start(cmd)
	//DPrintf("Get call raft done %d %d ",index,term)
	if isLeader == false || index == -1 || term == -1{
		reply.Err = ErrWrongLeader
		return
	} 
 
	kv.mu.Lock()
	waitList,ok := kv.wait[index]
	if !ok{
		waitList = list.New()
		kv.wait[index] = waitList
	}
	group := new(LogGroup)
	group.index = index
	group.term = term
	group.cmd = cmd
	group.getArgs = args
	group.getReply = reply
	group.done = false
	group.condition = sync.NewCond(&group.mutex)
	group.createTime = time.Now()
	waitList.PushBack(group)
	kv.mu.Unlock()
	group.mutex.Lock()
	for !group.done{
		DPrintf("%d Get :client %d reqNo %d optype %d key [%s] value [%s] groupAddr %p",
			kv.me,cmd.Client,cmd.ReqNumber,cmd.OpType,cmd.Key,cmd.Value,group)
		group.condition.Wait()
		DPrintf("Get notified %d %p",kv.me,group)
	}
	group.mutex.Unlock()
}

func (kv *KVServer)  PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.    
	var cmd Op
	if args.Op == "Put"{    
		cmd = Op{args.Client,args.ReqNumber,PUT,args.Key,args.Value}
	}else{
		cmd = Op{args.Client,args.ReqNumber,APPEND,args.Key,args.Value}
	}
 
	//DPrintf("PutAppend call raft")
	index,term,isLeader := kv.rf.Start(cmd)
	//DPrintf("PutAppend call raft done %d %d",index,term)

	if isLeader == false || index == -1 || term == -1{
		reply.Err = ErrWrongLeader
		return
	} 
 
	kv.mu.Lock()
	waitList,ok := kv.wait[index]
	if !ok{
		waitList = list.New()
		kv.wait[index] = waitList
	}
	group := new(LogGroup)
	group.index = index
	group.term = term
	group.cmd = cmd
	group.pAArgs = args
	group.pAReply = reply
	group.done = false
	group.condition = sync.NewCond(&group.mutex)
	group.createTime = time.Now()
	waitList.PushBack(group)
	kv.mu.Unlock()
	group.mutex.Lock()
	for !group.done{
		DPrintf("%d PutAppend :client %d reqNo %d optype %d key [%s] value [%s] groupAddr %p",
			kv.me,cmd.Client,cmd.ReqNumber,cmd.OpType,cmd.Key,cmd.Value,group)
		group.condition.Wait()
		DPrintf("PutAppend notified %d %p",kv.me,group)
	}
	group.mutex.Unlock()
}   
 
//
func ( kv *KVServer)DoReply(log *LogGroup,err Err){
	if log == nil {
		return 
	}
	log.mutex.Lock()
	if log.cmd.OpType == GET{
		log.getReply.Err = err
	}else{
		log.pAReply.Err =  err   
	} 
	log.done = true
	log.mutex.Unlock()
	log.condition.Signal()
	DPrintf("%d notify1 %p",kv.me,log)
}

//
func (kv *KVServer)DoReplyValue(log *LogGroup,err Err,value string){
	if log == nil {
		return 
	}  
	log.mutex.Lock()
	if log.cmd.OpType == GET{
		log.getReply.Err = err
		log.getReply.Value = value
	}else{
		log.pAReply.Err = err
	} 
	log.done = true
	log.mutex.Unlock()
	log.condition.Signal()
	DPrintf("%d notify2 %p",kv.me,log)
}

func (kv *KVServer)ApplyStateMachineRoutine(){ 
	for atomic.LoadInt32(&kv.dead) == 0 {
		//DPrintf("wait a applied msg")
		msg,ok := <-kv.applyCh
		if !ok{ 
			break 
		} 
		//DPrintf("%d applied msg",kv.me)
		kv.mu.Lock()
		//notify all previous index
		for i:=0;i<msg.CommandIndex;i++{
			prevList,ok := kv.wait[i]
			if !ok{
				delete(kv.wait,i)
				continue
			}
			for e := prevList.Front();e!= nil;e=e.Next(){
				W := e.Value.(*LogGroup)
				kv.DoReply(W,Outdated)
			}
			for prevList.Len() > 0{
				prevList.Remove(prevList.Front())
			}
			delete(kv.wait,i)
		}

		waitList,ok := kv.wait[msg.CommandIndex]
		if !ok{
			//DPrintf("wai list is null")
			waitList=nil
		}
		var finalWait *LogGroup = nil
		if waitList != nil{ 
			for e := waitList.Front();e!= nil;e=e.Next(){
				W := e.Value.(*LogGroup)
				if W.index != msg.CommandIndex || W.term != msg.CommandTerm {
					kv.DoReply(W,LogChanged)
				}else{ 
					if  finalWait != nil{
						DPrintf("more than two client,wait on the same index")
						kv.DoReply(finalWait,ClientsOnSameIndex)
					}
					finalWait = W;
				}
			} 
	
			for  waitList.Len() > 0{
				waitList.Remove(waitList.Front())
			}	
		}
 
		cmd := msg.Command.(Op)
		prevReqNumber,ok := kv.lastReqNumber[cmd.Client]
		if !ok{ 
			prevReqNumber = -1 
		}  
  
		var dup bool = false
		// cmd去 重
		if cmd.ReqNumber <= prevReqNumber{
			dup = true
		} else {
			kv.lastReqNumber[cmd .Client] = cmd.ReqNumber
		} 
		kv.mu.Unlock()
		DPrintf("%d apply msg :client %d reqNo %d optype %d key %s value %s",
			kv.me,cmd.Client,cmd.ReqNumber,cmd.OpType,cmd.Key,cmd.Value)
		if dup{ 
			if  cmd .OpType == GET {
				value,ok := kv.data[cmd.Key ] 
				if !ok{
				 	kv.DoReply(finalWait,ErrNoKey)
				}else{
					kv.DoReplyValue(finalWait,OK,value)
				 } 
			}else{ 
				kv.DoReply(finalWait,Duplicate)
			}
		}else{ 
			if  cmd .OpType == GET{
				value,ok := kv.data[cmd.Key]
				if !ok{ 
					kv.DoReply(finalWait,ErrNoKey)
				}else{
					kv.DoReplyValue(finalWait,OK,value)
				}
			}else if cmd.OpType == PUT{
				kv.data[cmd.Key] = cmd.Value
				kv.DoReply(finalWait,OK)
			}else if cmd.OpType == APPEND{
				value,ok := kv.data[cmd.Key]
				if !ok{
					kv.data[cmd.Key] = cmd.Value
					kv.DoReply(finalWait,OK)
				}else{
					kv.data[cmd.Key] = value + cmd.Value
					kv.DoReply(finalWait,OK)
				}
			}
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func (kv *KVServer) RpcTimeoutTimer() {
	for atomic.LoadInt32(&kv.dead) == 0 {
		//DPrintf("TimeoutTimer running")
		time.Sleep(time.Duration(kv.rpcTimeout) * time.Millisecond)
		kv.mu.Lock()
		for _,waitList := range kv.wait{
			for e := waitList.Front();e!= nil;{
				W := e.Value.(*LogGroup)
				if time.Since(W.createTime).Milliseconds() >= int64(kv.rpcTimeout) {
					kv.DoReply(W,RpcTimeout)
					next:= e.Next()
					waitList.Remove(e)
					e=next
				}else{
					e=e.Next()
				}
				
			}
		}
		kv.mu.Unlock()
	}
	DPrintf("RpcTimeoutTimer Done")
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.raftPersister = persister
	
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.InitKVServer()
	kv.mu.Lock()
	kv.mu.Unlock()
	return kv
}
