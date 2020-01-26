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

import "sync"
import "sync/atomic"
import "labrpc"

import "bytes"
import "labgob"

import "time"
import "math/rand"
import "container/list"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

type Entry struct {
	Term    int
	Command interface{}
	Index   int
}

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//Persistent state on all servers:
	CurrentTerm int
	VotedFor    int
	Log         []Entry

	//Volatile state on all servers:
	commitIndex int
	lastApplied int

	//Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	//others
	role    int
	randGen *rand.Rand

	//timers
	electionTimeoutBegin int //Millisecond
	electionTimeoutEnd   int //Millisecond
	electionReset        int32
	heartbeatTimeout     int //Millisecond

	//As a RPC Receiver
	//RPC handler will operate these data
	//handle vote requests
	voteReqs         *list.List
	voteReqMutex     sync.Mutex
	voteReqCondition *sync.Cond
	voteReqDone      bool

	//handle vote reply
	voteReplies        *list.List
	voteReplyMutex     sync.Mutex
	voteReplyCondition *sync.Cond
	voteReplyDone      bool

	//vote me count
	voteMeCount int

	//On Conversion To Candidate
	toCandidate bool

	//On Conversion To Leader
	toLeader bool
	number   int //for testing

	//handle AppendEntries requests
	aeReqs         *list.List
	aeReqMutex     sync.Mutex
	aeReqCondition *sync.Cond
	aeReqDone      bool

	//handle AppendEntries replies
	aeReplies        *list.List
	aeReplyMutex     sync.Mutex
	aeReplyCondition *sync.Cond
	aeReplyDone      bool

	//As a RPC sender
	//RequestVote Sender will operate these data
	peerVoteReqs         []*list.List
	peerVoteReqMutex     []sync.Mutex
	peerVoteReqCondition []*sync.Cond
	peerVoteReqDone      []bool

	//AppendEntries Sender will operate these data
	peerAeReqs         []*list.List
	peerAeReqMutex     []sync.Mutex
	peerAeReqCondition []*sync.Cond
	peerAeReqDone      []bool
	//When last log index ≥ nextIndex,
	//AppendEntries should been sent. But to prevent duplicate sending,
	//we want a label for the peer indicating this action.
	//after heartbeat timeout or AE reply from the peer,we reset labels.
	//And it can reduce numbers of AE RPC .
	peerAeReqAcking []bool

	//the time of sending AppendRequest
	peerAeReqSendTime []time.Time

	//the max time between two AppendRequest
	peerAeReqSendMaxInterval int64

	//the count of Entry sended in one AE RPC
	//It also can reduce numbers of AE RPC when its value is high.
	peerAeReqSendedCountInOneRPC int

	//ApplyM                                                                                                                        sg
	applyCh chan ApplyMsg
	applyMsgs         *list.List
	applyMsgMutex     sync.Mutex
	applyMsgCondition *sync.Cond
}

func (rf *Raft) InitRaft(peerCnt int) {
	DPrintf("Init Raft.")
	atomic.StoreInt32(&rf.dead, 0)
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = make([]Entry, 1)
	rf.Log[0] = Entry{0, nil, 0}

	rf.commitIndex = 0
	rf.lastApplied = 0

	DPrintf("peer cnt %d", peerCnt)
	rf.nextIndex = make([]int, peerCnt)
	rf.matchIndex = make([]int, peerCnt)

	rf.role = Follower
	/*
		In test, time.Now().UnixNano() may get the same value among different.
		callers of InitRaft(). So, it should be added some random value to prevent
		the situation.
	*/
	seed := time.Now().UnixNano() + int64(rand.Int()) + int64(rand.Int())
	DPrintf("seed %d", seed)
	rf.randGen = rand.New(rand.NewSource(seed))
	rf.electionTimeoutBegin = 600
	rf.electionTimeoutEnd = 750
	atomic.StoreInt32(&rf.electionReset, 0)
	rf.heartbeatTimeout = 150

	rf.voteReqs = list.New()
	rf.voteReqCondition = sync.NewCond(&rf.voteReqMutex)
	rf.voteReqDone = false

	rf.voteReplies = list.New()
	rf.voteReplyCondition = sync.NewCond(&rf.voteReplyMutex)
	rf.voteReplyDone = false

	rf.voteMeCount = 0
	rf.toCandidate = false
	rf.toLeader = false
	rf.number = 0

	rf.aeReqs = list.New()
	rf.aeReqCondition = sync.NewCond(&rf.aeReqMutex)
	rf.aeReqDone = false

	rf.aeReplies = list.New()
	rf.aeReplyCondition = sync.NewCond(&rf.aeReplyMutex)
	rf.aeReplyDone = false

	//init peer sender routine
	rf.peerVoteReqs = make([]*list.List, peerCnt)
	rf.peerVoteReqMutex = make([]sync.Mutex, peerCnt)
	rf.peerVoteReqCondition = make([]*sync.Cond, peerCnt)
	rf.peerVoteReqDone = make([]bool, peerCnt)
	
	for i := 0; i < peerCnt; i++ {
		rf.peerVoteReqs[i] = list.New()
		//DPrintf("peerVoteReqs[i] len %d ", rf.peerVoteReqs[i].Len())
		rf.peerVoteReqCondition[i] = sync.NewCond(&rf.peerVoteReqMutex[i])
		rf.peerVoteReqDone[i] = false
	}

	//init peer AppendEntries sender routine
	rf.peerAeReqs = make([]*list.List, peerCnt)
	rf.peerAeReqMutex = make([]sync.Mutex, peerCnt)
	rf.peerAeReqCondition = make([]*sync.Cond, peerCnt)
	rf.peerAeReqDone = make([]bool, peerCnt)
	rf.peerAeReqAcking = make([]bool, peerCnt)
	rf.peerAeReqSendTime = make([]time.Time,peerCnt)
	for i := 0; i < peerCnt; i++ {
		rf.peerAeReqs[i] = list.New()
		rf.peerAeReqCondition[i] = sync.NewCond(&rf.peerAeReqMutex[i])
		rf.peerAeReqDone[i] = false
		rf.peerAeReqAcking[i] = false
		rf.peerAeReqSendTime[i] = time.Now()
	}
	rf.peerAeReqSendedCountInOneRPC = 5
	rf.peerAeReqSendMaxInterval = int64(rf.heartbeatTimeout / 5)

	rf.applyMsgs = list.New()
	rf.applyMsgCondition = sync.NewCond(&rf.applyMsgMutex)

	DPrintf("Init ElectionTimer. electionTimeoutBegin %d", rf.electionTimeoutBegin)
	go rf.ElectionTimer()
	go rf.HeartbeatTimer()

	for i := 0; i < peerCnt; i++ {
		go rf.SendRequestVoteRoutine(i)
	}

	for i := 0; i < peerCnt; i++ {
		go rf.SendAppendEntriesRoutine(i)
	}

	go rf.PersistStateRoutine()
	go rf.ApplyMsgRoutine()
	go rf.PeerMainRoutine()
}

//destroy Raft
func (rf *Raft) DestroyRaft() {

}

//
//Reset Election Timer
func (rf *Raft) ResetElectionTimer() {
	atomic.StoreInt32(&rf.electionReset, 0)
}

//
//Election Timer
func (rf *Raft) ElectionTimer() {
	for atomic.LoadInt32(&rf.dead) == 0 {
		//DPrintf("ElectionTimer running")

		//enable election timer
		//rf.randGen.Seed(time.Now().UnixNano())
		timeout := rf.electionTimeoutBegin + int(float32(rf.electionTimeoutEnd-rf.electionTimeoutBegin)*rf.randGen.Float32())
		atomic.StoreInt32(&rf.electionReset, 1)
		time.Sleep(time.Duration(timeout) * time.Millisecond)

		//process timeout
		if atomic.LoadInt32(&rf.electionReset) == 1 {
			rf.Lock()
			if rf.role == Follower {
				//DPrintf("will convert to candidate")
				//will convert to candidate
				rf.OnConversionToCandidate()
			} else if rf.role == Candidate {
				if rf.voteMeCount <= len(rf.peers)/2 {
					//start new election
					DPrintf("ElectionTimeout %d millis, %d to candidate again", timeout, rf.me)
					rf.OnConversionToCandidate()
				}
			}

			rf.Unlock()
		}
	}
	DPrintf("ElectionTimer Done")
}

//heartbeat Timer
func (rf *Raft) HeartbeatTimer() {
	for atomic.LoadInt32(&rf.dead) == 0 {
		//DPrintf("HeartbeatTimer running")
		time.Sleep(time.Duration(rf.heartbeatTimeout) * time.Millisecond)

		rf.Lock()
		if rf.role == Leader {
			rf.SendHeartbeatToPeers()

			//reset AppendEntries Acking labels for next sending
			for i := 0; i < len(rf.peers); i++ {
				rf.peerAeReqAcking[i] = false
			}
		}
		rf.Unlock()
	}
	DPrintf("HeartbeatTimer Done")
}

func (rf *Raft) Lock() {
	rf.mu.Lock()
}

func (rf *Raft) Unlock() {
	rf.mu.Unlock()
}

func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

//Main process loop for peer
func (rf *Raft) PeerMainRoutine() {
	for atomic.LoadInt32(&rf.dead) == 0 {
		//DPrintf("Main Routine is running")
		rf.Lock()
		if rf.role == Follower {
			again := rf.FollowerRoutine()
			if again {
				rf.Unlock()
				continue
			}
		} else if rf.role == Candidate {
			again := rf.CandidateRoutine()
			if again {
				rf.Unlock()
				continue
			}
		} else {
			//must be the leader
			again := rf.LeaderRoutine()
			if again {
				rf.Unlock()
				continue
			}
		}
		rf.Unlock()
		time.Sleep(1 * time.Millisecond)
	}
}

func (rf *Raft) OnConversionToFollower() {
	rf.role = Follower
	rf.VotedFor = -1
	DPrintf("ToFollower %d Term %d", rf.me, rf.CurrentTerm)
}

//Follower routine
//return true -- will entry routine again
func (rf *Raft) FollowerRoutine() bool {
	//handle RequestVote Request
	rf.voteReqMutex.Lock()
	rf.voteReqDone = false
	if rf.voteReqs.Len() > 0 {
		req := rf.voteReqs.Front().Value.(VoteGroup)
		if req.args.Term > rf.CurrentTerm {
			rf.CurrentTerm = req.args.Term
			rf.OnConversionToFollower()
			rf.voteReqMutex.Unlock()
			return true
		} else if req.args.Term < rf.CurrentTerm {
			req.reply.Term = req.args.Term
			req.reply.VoteGranted = false
			rf.voteReqDone = true
			rf.voteReqs.Remove(rf.voteReqs.Front())
		} else {
			req.reply.Term = req.args.Term
			if (rf.VotedFor == -1 || rf.VotedFor == req.args.CandidateId) && !rf.MyLogIsNewer(req.args) {
				req.reply.VoteGranted = true
				DPrintf("Follower %d grant RV %d %d", rf.me, req.args.CandidateId, req.args.Term)
				rf.VotedFor = req.args.CandidateId
				rf.ResetElectionTimer()
			} else {
				req.reply.VoteGranted = false
			}
			rf.voteReqDone = true
			rf.voteReqs.Remove(rf.voteReqs.Front())
		}
	}
	rf.voteReqMutex.Unlock()
	rf.voteReqCondition.Signal()

	//handle RequestVote Reply
	//之前是candidate时，发送的RequestVote RPC，现在才收到回复；
	rf.voteReplyMutex.Lock()
	rf.voteReplyDone = false
	if rf.voteReplies.Len() > 0 {
		rep := rf.voteReplies.Front().Value.(VoteGroup)
		if rep.args.Term != rf.CurrentTerm {
			DPrintf("Follower drop vote reply from previous terms")
			            rf.voteReplies.Remove(rf.voteReplies.Front())
		}else if rep.reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = rep.reply.Term
			rf.OnConversionToFollower()
			rf.voteReplyMutex.Unlock()
			return true
		} else if rep.reply.Term < rf.CurrentTerm {
			//Drop rpc response
			rf.voteReplies.Remove(rf.voteReplies.Front())
		} else {
			//Drop rpc response
			rf.voteReplies.Remove(rf.voteReplies.Front())
		}
	}
	rf.voteReplyMutex.Unlock()

	//handle AppendEntries Requests
	rf.aeReqMutex.Lock()
	rf.aeReqDone = false
	if rf.aeReqs.Len() > 0 {
		req := rf.aeReqs.Front().Value.(AeGroup)
		if req.args.Term > rf.CurrentTerm {
			DPrintf("AA")
			rf.CurrentTerm = req.args.Term
			rf.OnConversionToFollower()
			rf.aeReqMutex.Unlock()
			rf.ResetElectionTimer()
			return true
		} else if req.args.Term < rf.CurrentTerm {
			DPrintf("BB")
			req.reply.Term = rf.CurrentTerm
			req.reply.Success = false
			rf.aeReqDone = true
			rf.aeReqs.Remove(rf.aeReqs.Front())
		} else {
			rf.ResetElectionTimer()
			req.reply.Term = rf.CurrentTerm
			if req.args.PrevLogIndex >= len(rf.Log) {
				req.reply.Success = false
				req.reply.ConflictTerm = -1
				req.reply.FirstIndexOfConflictTerm = len(rf.Log)
			} else if rf.Log[req.args.PrevLogIndex].Term != req.args.PrevLogTerm {
				req.reply.Success = false
				req.reply.ConflictTerm = rf.Log[req.args.PrevLogIndex].Term
				i := req.args.PrevLogIndex
				for ; i >= 0; i-- {
					if rf.Log[i].Term != rf.Log[req.args.PrevLogIndex].Term {
						break
					}
				}
				req.reply.FirstIndexOfConflictTerm = i + 1
			} else {
				newIndex := req.args.PrevLogIndex + 1
				offset := newIndex
				newLen := Min(len(rf.Log), offset+len(req.args.Entries))

				//it must be the index of last new entry.
				indexOfLastNewEntry := 0
				//find entry conflicts
				for ; newIndex < newLen; newIndex++ {
					if (rf.Log[newIndex].Index == req.args.Entries[newIndex-offset].Index) &&
						(rf.Log[newIndex].Term == req.args.Entries[newIndex-offset].Term) {
						continue
					} else {
						break
					}
				}
				//delete all conflicting entries and apply new entries
				if newIndex < newLen {
					//删除冲突及其之后的全部entry
					delCnt := len(rf.Log) - newIndex
					rf.Log = rf.Log[:len(rf.Log)-delCnt]
					//拼接上leader的日志
					for index := newIndex - offset; index < len(req.args.Entries); index++ {
						rf.Log = append(rf.Log, req.args.Entries[index])
					}
					indexOfLastNewEntry = newLen - 1
				} else if len(rf.Log) < offset+len(req.args.Entries) {
					//拼接上新增的日志
					for index := newIndex - offset; index < len(req.args.Entries); index++ {
						rf.Log = append(rf.Log, req.args.Entries[index])
					}
					indexOfLastNewEntry = len(rf.Log) - 1
				} else {
					indexOfLastNewEntry = offset + len(req.args.Entries) - 1
				}
				if req.args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = Min(req.args.LeaderCommit, indexOfLastNewEntry)
				}
				req.reply.Success = true
			}
			rf.aeReqDone = true

			if req.reply.Success {
				//DPrintf("{%d Term %d} CC AeReply {%d Term %d}number %d Success,LeaderCommit %d myCommitIndex %d mylastIndex %d", rf.me, rf.CurrentTerm,req.args.LeaderId,req.args.Term, req.args.Number, req.args.LeaderCommit, rf.commitIndex, len(rf.Log)-1)
			} else {
				//DPrintf("{%d Term %d} CC AeReply {%d Term %d}number %d Fail,ConflictTerm %d FirstIndex %d", rf.me, rf.CurrentTerm,req.args.LeaderId,req.args.Term, req.args.Number,req.reply.ConflictTerm,req.reply.FirstIndexOfConflictTerm)
			}
			rf.aeReqs.Remove(rf.aeReqs.Front())
		}
	}
	rf.aeReqMutex.Unlock()
	rf.aeReqCondition.Signal()

	//handle AppendEntries reply
	//之前是leader时，发送的AppendEntries RPC，现在才收到回复；
	rf.aeReplyMutex.Lock()
	rf.aeReplyDone = false
	if rf.aeReplies.Len() > 0 {
		rep := rf.aeReplies.Front().Value.(AeGroup)
		//ACK Done
		rf.peerAeReqAcking[rep.peer] = false

		if rep.args.Term != rf.CurrentTerm {
			DPrintf("EEE Follower drop Ae reply from previous terms")
			            rf.aeReplies.Remove(rf.aeReplies.Front())
		}else if rep.reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = rep.reply.Term
			rf.OnConversionToFollower()
			rf.aeReplyMutex.Unlock()
			return true
		} else if rep.reply.Term < rf.CurrentTerm {
			//Drop rpc response
			rf.aeReplies.Remove(rf.aeReplies.Front())
		} else {
			//Drop rpc response
			rf.aeReplies.Remove(rf.aeReplies.Front())
		}
	}
	rf.aeReplyMutex.Unlock()

	if rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		//Apply State.log[State.lastApplied] to state machine;
		msg := new(ApplyMsg)
		msg.CommandValid = true
		msg.Command = rf.Log[rf.lastApplied].Command
		msg.CommandIndex = rf.Log[rf.lastApplied].Index
		msg.CommandTerm = rf.Log[rf.lastApplied].Term
		rf.applyMsgMutex.Lock()
		rf.applyMsgs.PushBack(msg)
		rf.applyMsgMutex.Unlock()
		rf.applyMsgCondition.Signal()
	}

	return false
}

//my log is newer
func (rf *Raft) MyLogIsNewer(args *RequestVoteArgs) bool {
	if rf.Log[len(rf.Log)-1].Term > args.LastLogTerm {
		return true
	} else if (args.LastLogTerm == rf.Log[len(rf.Log)-1].Term) && (len(rf.Log) > args.LastLogIndex+1) {
		return true
	}
	return false
}

//Candidate routine
func (rf *Raft) CandidateRoutine() bool {
	//On conversion to Candidate
	if rf.toCandidate {
		DPrintf("ToCandidate %d Term %d", rf.me, rf.CurrentTerm)
		//Send RequestVote RPCs to all other servers;
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			args := new(RequestVoteArgs)
			args.Term = rf.CurrentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = len(rf.Log) - 1
			args.LastLogTerm = rf.Log[args.LastLogIndex].Term
			reply := new(RequestVoteReply)
			rf.peerVoteReqMutex[i].Lock()
			rf.peerVoteReqs[i].PushBack(VoteGroup{args, reply, i})
			rf.peerVoteReqMutex[i].Unlock()
			rf.peerVoteReqCondition[i].Signal()
		}
		rf.toCandidate = false
	}

	//handle RequestVote Request
	rf.voteReqMutex.Lock()
	rf.voteReqDone = false
	//DPrintf("%d will handle RV request, reqs count %d", rf.me, rf.voteReqs.Len())
	if rf.voteReqs.Len() > 0 {
		req := rf.voteReqs.Front().Value.(VoteGroup)
		if req.args.Term > rf.CurrentTerm {
			rf.CurrentTerm = req.args.Term
			rf.OnConversionToFollower()
			rf.voteReqMutex.Unlock()
			return true
		} else if req.args.Term < rf.CurrentTerm {
			req.reply.Term = req.args.Term
			req.reply.VoteGranted = false
			rf.voteReqDone = true
			rf.voteReqs.Remove(rf.voteReqs.Front())
		} else {
			req.reply.Term = req.args.Term
			if (rf.VotedFor == -1 || rf.VotedFor == req.args.CandidateId) && !rf.MyLogIsNewer(req.args) {
				req.reply.VoteGranted = true
				DPrintf("Candidate %d grant RV %d %d", rf.me, req.args.CandidateId, req.args.Term)
				rf.VotedFor = req.args.CandidateId
				rf.ResetElectionTimer()
			} else {
				req.reply.VoteGranted = false
			}
			rf.voteReqDone = true
			rf.voteReqs.Remove(rf.voteReqs.Front())
		}
	}
	rf.voteReqMutex.Unlock()
	rf.voteReqCondition.Signal()

	//handle RequestVote Reply
	rf.voteReplyMutex.Lock()
	rf.voteReplyDone = false
	//DPrintf("%d will handle RV reply,reps count %d", rf.me, rf.voteReplies.Len())
	if rf.voteReplies.Len() > 0 {
		rep := rf.voteReplies.Front().Value.(VoteGroup)
		//DPrintf("%s %s", *rep.args, *rep.reply)
		if rep.args.Term != rf.CurrentTerm {
			           DPrintf("Candidate drop vote reply from previous terms")
			 rf.voteReplies.Remove(rf.voteReplies.Front())
		}else if rep.reply.Term > rf.CurrentTerm {
			DPrintf("A %d", rf.me)
			rf.CurrentTerm = rep.reply.Term
			rf.OnConversionToFollower()
			rf.voteReplyMutex.Unlock()
			return true
		} else if rep.reply.Term < rf.CurrentTerm {
			//Drop rpc response
			DPrintf("B %d", rf.me)
			rf.voteReplies.Remove(rf.voteReplies.Front())
		} else {
			if rf.CurrentTerm != rep.args.Term {
				//Drop rpc response
				DPrintf("C %d", rf.me)
				rf.voteReplies.Remove(rf.voteReplies.Front())
			} else {
				if rep.reply.VoteGranted {
					DPrintf("D %d vote %d", rep.peer, rf.me)
					rf.voteMeCount++
				} else {
					DPrintf("E %d", rf.me)
				}
				rf.voteReplies.Remove(rf.voteReplies.Front())
			}
		}
	}
	rf.voteReplyMutex.Unlock()

	//handle AppendEntries Requests
	rf.aeReqMutex.Lock()
	rf.aeReqDone = false
	//DPrintf("%d will handle AE request,req count %d", rf.me, rf.aeReqs.Len())
	if rf.aeReqs.Len() > 0 {
		req := rf.aeReqs.Front().Value.(AeGroup)
		if req.args.Term >= rf.CurrentTerm {
			rf.CurrentTerm = req.args.Term
			rf.OnConversionToFollower()
			rf.aeReqMutex.Unlock()
			rf.ResetElectionTimer()
			return true
		} else {
			//表示已有leader的term比State.currentTerm小，按要求要返回false；
			req.reply.Term = rf.CurrentTerm
			req.reply.Success = false
			rf.aeReqDone = true
			rf.aeReqs.Remove(rf.aeReqs.Front())
		}
	}
	rf.aeReqMutex.Unlock()
	rf.aeReqCondition.Signal()

	//handle AppendEntries reply
	//之前是leader时，发送的AppendEntries RPC，现在才收到回复；
	rf.aeReplyMutex.Lock()
	rf.aeReplyDone = false
	//DPrintf("%d will handle AE reply,reps count %d", rf.me, rf.aeReplies.Len())
	if rf.aeReplies.Len() > 0 {
		rep := rf.aeReplies.Front().Value.(AeGroup)
		//ACK Done
		rf.peerAeReqAcking[rep.peer] = false

		if rep.args.Term != rf.CurrentTerm {
			           DPrintf("EEE Candidate drop Ae reply from previous terms")
			 rf.aeReplies.Remove(rf.aeReplies.Front())
		}else if rep.reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = rep.reply.Term
			rf.OnConversionToFollower()
			rf.aeReplyMutex.Unlock()
			return true
		} else if rep.reply.Term < rf.CurrentTerm {
			//Drop rpc response
			rf.aeReplies.Remove(rf.aeReplies.Front())
		} else {
			//表示之前是leader时，发送的AppendEntries，现在才收到回复.
			//Drop rpc response
			rf.aeReplies.Remove(rf.aeReplies.Front())
		}
	}
	rf.aeReplyMutex.Unlock()

	//DPrintf("%d voteMeCount %d", rf.me, rf.voteMeCount)
	if rf.voteMeCount > (len(rf.peers) / 2) {
		//去执行leader的逻辑;
		//convert to leader
		rf.OnConversionToLeader()
		return true
	}

	if rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		//Apply State.log[State.lastApplied] to state machine;
		msg := new(ApplyMsg)
		msg.CommandValid = true
		msg.Command = rf.Log[rf.lastApplied].Command
		msg.CommandIndex = rf.Log[rf.lastApplied].Index
		msg.CommandTerm = rf.Log[rf.lastApplied].Term
		rf.applyMsgMutex.Lock()
		rf.applyMsgs.PushBack(msg)
		rf.applyMsgMutex.Unlock()
		rf.applyMsgCondition.Signal()
	}

	return false
}

func (rf *Raft) OnConversionToCandidate() {
	rf.role = Candidate
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.voteMeCount = 1
	rf.ResetElectionTimer()
	rf.toCandidate = true
}

//send RequestVote To Who
func (rf *Raft) SendRequestVoteRoutine(toWho int) {
	for atomic.LoadInt32(&rf.dead) == 0 {
		//DPrintf("SendRequestVote Routine is running")
		rf.peerVoteReqMutex[toWho].Lock()
		for rf.peerVoteReqs[toWho].Len() <= 0 {
			rf.peerVoteReqCondition[toWho].Wait()
		}

		req := rf.peerVoteReqs[toWho].Front().Value.(VoteGroup)
		rf.peerVoteReqs[toWho].Remove(rf.peerVoteReqs[toWho].Front())
		rf.peerVoteReqMutex[toWho].Unlock()
		//DPrintf("send RV from %d to %d", rf.me, toWho)
		//sendRequestVote will block the call Routine. Put it in a inner routine
		go func() {
			ret := rf.sendRequestVote(toWho, req.args, req.reply)
			if ret {
				//如果有回复，放入回复链表
				rf.voteReplyMutex.Lock()
				//DPrintf("RV reply from %d to %d", toWho, rf.me)
				rf.voteReplies.PushBack(req)
				rf.voteReplyMutex.Unlock()
			} else {
				//Drop Request ?
				//DPrintf("RV timeout from %d to %d", rf.me, toWho)
			}
		}()

		time.Sleep(1 * time.Millisecond)
	}
}

//Leader routine
func (rf *Raft) LeaderRoutine() bool {
	if rf.toLeader {
		DPrintf("ToLeader %d Term %d ", rf.me, rf.CurrentTerm)
		//Send initial heartbeat to each server;
		rf.SendHeartbeatToPeers()
		rf.toLeader = false
	}
	//handle RequestVote Request
	rf.voteReqMutex.Lock()
	rf.voteReqDone = false
	if rf.voteReqs.Len() > 0 {
		req := rf.voteReqs.Front().Value.(VoteGroup)
		if req.args.Term > rf.CurrentTerm {
			rf.CurrentTerm = req.args.Term
			rf.OnConversionToFollower()
			rf.voteReqMutex.Unlock()
			return true
		} else if req.args.Term < rf.CurrentTerm {
			req.reply.Term = req.args.Term
			req.reply.VoteGranted = false
			rf.voteReqDone = true
			rf.voteReqs.Remove(rf.voteReqs.Front())
		} else {
			req.reply.Term = req.args.Term
			if (rf.VotedFor == -1 || rf.VotedFor == req.args.CandidateId) && !rf.MyLogIsNewer(req.args) {
				req.reply.VoteGranted = true
				DPrintf("Leader %d grant RV %d %d", rf.me, req.args.CandidateId, req.args.Term)
				rf.VotedFor = req.args.CandidateId
				rf.ResetElectionTimer()
			} else {
				req.reply.VoteGranted = false
			}
			rf.voteReqDone = true
			rf.voteReqs.Remove(rf.voteReqs.Front())
		}
	}
	rf.voteReqMutex.Unlock()
	rf.voteReqCondition.Signal()

	//handle RequestVote Reply
	//之前是candidate时，发送的RequestVote RPC，现在才收到回复；
	rf.voteReplyMutex.Lock()
	rf.voteReplyDone = false
	if rf.voteReplies.Len() > 0 {
		rep := rf.voteReplies.Front().Value.(VoteGroup)
		if rep.args.Term != rf.CurrentTerm {
			 DPrintf("Leader drop vote reply from previous terms")
			 rf.voteReplies.Remove(rf.voteReplies.Front())
		}else if rep.reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = rep.reply.Term
			rf.OnConversionToFollower()
			rf.voteReplyMutex.Unlock()
			return true
		} else if rep.reply.Term < rf.CurrentTerm {
			//Drop rpc response
			rf.voteReplies.Remove(rf.voteReplies.Front())
		} else {
			//已经是leader了，多余的投票无关紧要了
			//Drop rpc response
			rf.voteReplies.Remove(rf.voteReplies.Front())
		}
	}
	rf.voteReplyMutex.Unlock()

	//handle AppendEntries Requests
	rf.aeReqMutex.Lock()
	rf.aeReqDone = false
	if rf.aeReqs.Len() > 0 {
		req := rf.aeReqs.Front().Value.(AeGroup)
		if req.args.Term > rf.CurrentTerm {
			rf.CurrentTerm = req.args.Term
			rf.OnConversionToFollower()
			rf.aeReqMutex.Unlock()
			rf.ResetElectionTimer()
			return true
		} else if req.args.Term < rf.CurrentTerm {
			req.reply.Term = rf.CurrentTerm
			req.reply.Success = false
			rf.aeReqDone = true
			rf.aeReqs.Remove(rf.aeReqs.Front())
		} else {
			rf.CurrentTerm = req.args.Term
			rf.OnConversionToFollower()
			rf.aeReqMutex.Unlock()
			rf.ResetElectionTimer()
			return true
		}
	}
	rf.aeReqMutex.Unlock()
	rf.aeReqCondition.Signal()

	//handle AppendEntries reply
	rf.aeReplyMutex.Lock()
	rf.aeReplyDone = false
	if rf.aeReplies.Len() > 0 {
		rep := rf.aeReplies.Front().Value.(AeGroup)
		
		//ACK Done
		rf.peerAeReqAcking[rep.peer] = false

		if rep.args.Term != rf.CurrentTerm {
			 DPrintf("EEE Leader drop Ae reply from previous terms")
			 rf.aeReplies.Remove(rf.aeReplies.Front())
		}else if rep.reply.Term > rf.CurrentTerm {
			DPrintf("AAA")
			rf.CurrentTerm = rep.reply.Term
			rf.OnConversionToFollower()
			rf.aeReplyMutex.Unlock()
			return true
		} else if rep.reply.Term < rf.CurrentTerm {
			if rep.reply.Success {
				DPrintf("BBB reply from {%d Term %d} to {%d Term %d} Success", rep.peer, rep.reply.Term, rf.me, rf.CurrentTerm)
			} else {
				DPrintf("BBB reply from {%d Term %d} to {%d Term %d} Fail", rep.peer, rep.reply.Term, rf.me, rf.CurrentTerm)
			}

			//之前是leader时，发送的AppendEntries，现在才得到回复
			//Drop rpc response
			rf.aeReplies.Remove(rf.aeReplies.Front())
		} else if rep.reply.Success {
			//DPrintf("CCC")
			//Update State.nextIndex[A follower] and State.matchIndex[A follower];
			//which follower?
			prev := rf.matchIndex[rep.peer]
			next := rep.args.PrevLogIndex + len(rep.args.Entries)
			next = Max(next, prev)
			rf.matchIndex[rep.peer] = next
			rf.nextIndex[rep.peer] = Max(rf.matchIndex[rep.peer]+1, rf.nextIndex[rep.peer])
			//DPrintf("CCC number %d from %d to matchIndex[%d] %d nextIndex[%d] %d", rep.args.Number, prev, rep.peer, rf.matchIndex[rep.peer], rep.peer, rf.nextIndex[rep.peer])
			rf.aeReplies.Remove(rf.aeReplies.Front())
		} else {

			//Decement State.nextIndex[A follower] and retry;
			//prev := rf.nextIndex[rep.peer]
			next := rf.nextIndex[rep.peer]
			if rep.reply.ConflictTerm != -1 {
				i := rep.args.PrevLogIndex
				for ; i >= 0; i-- {
					if rf.Log[i].Term == rep.reply.ConflictTerm {
						break
					}
				}
				//找到conflictTerm最后一个entry
				if i >= 0 {
					next = i + 1
					//DPrintf("AAAA i+1 %d prevLogIndex %d lastIndex %d",i+1,rep.args.PrevLogIndex,len(rf.Log)-1)
				} else {
					next = rep.reply.FirstIndexOfConflictTerm
					//DPrintf("BBBB firstIndex %d prevLogIndex %d lastIndex %d",rep.reply.FirstIndexOfConflictTerm,rep.args.PrevLogIndex,len(rf.Log)-1)
				}
			} else {
				next = rep.reply.FirstIndexOfConflictTerm
				//DPrintf("CCCC firstIndex %d prevLogIndex %d lastIndex %d",rep.reply.FirstIndexOfConflictTerm,rep.args.PrevLogIndex,len(rf.Log)-1)
			}

			next = Min(next, rf.nextIndex[rep.peer])
			rf.nextIndex[rep.peer] = Max(1, next)
			/*
				DPrintf("DDD from {%d Term %d} to {%d Term %d} number %d , nextIndex[%d] backup from %d to %d ",
					rep.peer,rep.reply.Term,rf.me,rf.CurrentTerm,
					rep.args.Number, rep.peer, prev, rf.nextIndex[rep.peer])
			*/
			rf.aeReplies.Remove(rf.aeReplies.Front())
		}
	}
	rf.aeReplyMutex.Unlock()

	//send AppendEntries to peers which are outdated
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if len(rf.Log)-1 >= rf.nextIndex[i] {          
			if rf.peerAeReqAcking[i] { 
				if(time.Since(rf.peerAeReqSendTime[i]).Milliseconds() >= rf.peerAeReqSendMaxInterval){
					 rf.peerAeReqAcking[i] = false
				}else{
					continue
				}
			}

			//Send AppendEntries RPC with log entries starting at State.nextIndex[A follower];
			args := new(AppendEntriesArgs)
			args.Term = rf.CurrentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
			for j := rf.nextIndex[i]; j < Min(len(rf.Log), rf.nextIndex[i]+rf.peerAeReqSendedCountInOneRPC); j++ {
				args.Entries = append(args.Entries, rf.Log[j])
			}

			args.LeaderCommit = rf.commitIndex
			args.Number = rf.number
			rf.number++

			reply := new(AppendEntriesReply)

			//set ACKing label for the peer
			rf.peerAeReqAcking[i] = true
			rf.peerAeReqSendTime[i] = time.Now()

			rf.peerAeReqMutex[i].Lock()
			rf.peerAeReqs[i].PushBack(AeGroup{args, reply, i})
			//DPrintf("{%d term %d} send AE to %d number %d LeaderCommit %d myindex %d nextIndex[%d] %d",
			//	rf.me, rf.CurrentTerm, i, args.Number, args.LeaderCommit, len(rf.Log)-1, i, rf.nextIndex[i])

			rf.peerAeReqMutex[i].Unlock()
			rf.peerAeReqCondition[i].Signal()
		}
	}

	for N := rf.commitIndex + 1; N < len(rf.Log); N++ {
		if rf.Log[N].Term != rf.CurrentTerm {
			continue
		}

		cnt := 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= N {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			//Set commitIndex = N;
			rf.commitIndex = N
		}
	}

	if rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		//Apply State.log[State.lastApplied] to state machine;
		msg := new(ApplyMsg)
		msg.CommandValid = true
		msg.Command = rf.Log[rf.lastApplied].Command
		msg.CommandIndex = rf.Log[rf.lastApplied].Index
		msg.CommandTerm = rf.Log[rf.lastApplied].Term
		rf.applyMsgMutex.Lock()
		rf.applyMsgs.PushBack(msg)
		rf.applyMsgMutex.Unlock()
		rf.applyMsgCondition.Signal()
	}

	return false
}

func (rf *Raft) OnConversionToLeader() {
	rf.role = Leader
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.Log)
		rf.matchIndex[i] = 0
	}
	rf.toLeader = true
}

//send heartbeat to all peers
func (rf *Raft) SendHeartbeatToPeers() {
	//Send heartbeat to each server;
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := new(AppendEntriesArgs)
		args.Term = rf.CurrentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.nextIndex[i] - 1
		args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
		args.Entries = nil
		args.LeaderCommit = rf.commitIndex
		args.Number = rf.number
		rf.number++
		reply := new(AppendEntriesReply)

		//DPrintf("{%d Term %d}heartbeat to %d number %d ",rf.me,rf.CurrentTerm ,i, args.Number)

		rf.peerAeReqMutex[i].Lock()
		rf.peerAeReqs[i].PushBack(AeGroup{args, reply, i})
		rf.peerAeReqMutex[i].Unlock()
		rf.peerAeReqCondition[i].Signal()
	}
}

//send AppendEntries to peer RPC
func (rf *Raft) SendAppendEntriesRoutine(toWho int) {
	for atomic.LoadInt32(&rf.dead) == 0 {
		//DPrintf("SendAppendEntries Routine is running")
		rf.peerAeReqMutex[toWho].Lock()
		for rf.peerAeReqs[toWho].Len() <= 0 {
			rf.peerAeReqCondition[toWho].Wait()
		}

		req := rf.peerAeReqs[toWho].Front().Value.(AeGroup)
		rf.peerAeReqs[toWho].Remove(rf.peerAeReqs[toWho].Front())
		rf.peerAeReqMutex[toWho].Unlock()
		//DPrintf("send AE from %d to %d", rf.me, toWho)
		//sendAppendEntries will block the call Routine. Put it in a inner routine
		go func() {
			ret := rf.sendAppendEntries(toWho, req.args, req.reply)
			if ret {
				//如果有回复，放入回复链表
				rf.aeReplyMutex.Lock()
				/*
					if req.reply.Success {
						DPrintf("AE reply from %d to %d {%d Term %d} number %d Success args addr %p reply addr %p", toWho, rf.me,req.peer,req.reply.Term,req.args.Number,req.args,req.reply)
					}else{
						DPrintf("AE reply from %d to %d {%d Term %d} number %d Fail args addr %p reply addr %p", toWho, rf.me,req.peer,req.reply.Term,req.args.Number,req.args,req.reply)
					}
				*/

				rf.aeReplies.PushBack(req)
				rf.aeReplyMutex.Unlock()
			} else {
				//Drop Request ?
				//DPrintf("AE timeout from %d to %d",rf.me, toWho)
			}
		}()

		time.Sleep(1 * time.Millisecond)
	}
}
         
func (rf *Raft) ApplyMsgRoutine() {
	for atomic.LoadInt32(&rf.dead) == 0 {
		rf.applyMsgMutex.Lock() 
		for rf.applyMsgs.Len() <= 0{
			rf.applyMsgCondition.Wait()
		}
		msg := rf.applyMsgs.Front().Value.(*ApplyMsg)
		rf.applyMsgs.Remove(rf.applyMsgs.Front())
		rf.applyMsgMutex.Unlock()
		rf.applyCh <- *msg
		//DPrintf("Leader %d apply Entry[index %d value %d]",rf.me,msg.CommandIndex,msg.Command)
		time.Sleep(1 * time.Millisecond)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.Lock()
	term = rf.CurrentTerm
	isleader = (rf.role == Leader)
	rf.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.CurrentTerm)
	encoder.Encode(rf.VotedFor)
	encoder.Encode(rf.Log)
	output := writer.Bytes()
	rf.persister.SaveRaftState(output)
	//DPrintf("persist Done")
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintf("readpersist: data is null")
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var term int
	var voteFor int
	var entries []Entry
	if decoder.Decode(&term) != nil || decoder.Decode(&voteFor) != nil || decoder.Decode(&entries) != nil {
		DPrintf("Decode Raft failed")
	} else {
		rf.CurrentTerm = term
		rf.VotedFor = voteFor
		rf.Log = entries
	}
	DPrintf("readpersist Done")
}

func (rf *Raft) PersistStateRoutine() {
	//persistent state on all servers
	var prevCurrentTerm int = 0
	var prevVotedFor int = -1
	var prevLog []Entry = make([]Entry, 1)
	prevLog[0] = Entry{0, nil, 0}

	for atomic.LoadInt32(&rf.dead) == 0 {
		rf.Lock()
		if rf.StatesAreUpdated(prevCurrentTerm, prevVotedFor, prevLog) {
			//copy states out
			prevCurrentTerm = rf.CurrentTerm
			prevVotedFor = rf.VotedFor
			prevLog = make([]Entry, len(rf.Log))
			copy(prevLog, rf.Log)
			rf.persist()
		}
		rf.Unlock()

		time.Sleep(1 * time.Millisecond)
	}
}

func (rf *Raft) StatesAreUpdated(prevCurrentTerm int, prevVotedFor int, prevLog []Entry) bool {
	if prevCurrentTerm != rf.CurrentTerm {
		return true
	}
	if prevVotedFor != rf.VotedFor {
		return true
	}
	if len(prevLog) != len(rf.Log) {
		return true
	}
	for i := 0; i < len(rf.Log); i++ {
		//TODO:how to check differences between two command ?
		if prevLog[i].Index != rf.Log[i].Index || prevLog[i].Term != rf.Log[i].Term {
			return true
		}
	}
	return false
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type VoteGroup struct {
	args  *RequestVoteArgs
	reply *RequestVoteReply
	peer  int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
	Number       int
}

type AppendEntriesReply struct {
	Term                     int
	Success                  bool
	ConflictTerm             int
	FirstIndexOfConflictTerm int
}

type AeGroup struct {
	args  *AppendEntriesArgs
	reply *AppendEntriesReply
	peer  int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.voteReqMutex.Lock()
	rf.voteReqDone = false
	rf.voteReqs.PushBack(VoteGroup{args, reply, rf.me})
	for !rf.voteReqDone {
		rf.voteReqCondition.Wait()
	}
	rf.voteReqMutex.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.aeReqMutex.Lock()
	rf.aeReqDone = false
	rf.aeReqs.PushBack(AeGroup{args, reply, rf.me})
	for !rf.aeReqDone {
		rf.aeReqCondition.Wait()
	}
	rf.aeReqMutex.Unlock()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.Lock()
	term = rf.CurrentTerm
	isLeader = (rf.role == Leader)
	if isLeader {
		rf.Log = append(rf.Log, Entry{term, command, len(rf.Log)})
		index = len(rf.Log) - 1
		//DPrintf("Append log,index %d", index)
	}

	rf.Unlock()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	DPrintf("Kill Raft")
	rf.Lock()
	rf.persist()
	rf.Unlock()
	rf.DestroyRaft()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.InitRaft(len(peers))
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.Unlock()
	return rf
}
