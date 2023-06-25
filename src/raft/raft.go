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

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	logs          []Log         // Log entries
	currentTerm   int           // latest term server has seen
	votedFor      map[int]int   // candidateId that received vote in current term (or -1 if none) key: term value: votedFor
	commitIndex   int           // index of highest log entry known to be committed
	lastApplied   int           // index of highest log entry applied to state machine
	applyChan     chan ApplyMsg // channel of applied message
	lastHeartBeat time.Time     // last heart beat time

	curState RState // current state
}

type RState int // current state of this peer.
const (
	follower  RState = 1
	candidate RState = 2
	leader    RState = 3
)

type Log struct {
	term    int    // term of this Log
	index   int    // index of this Log
	content []byte // content of this log
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.curState == leader
	rf.mu.Unlock()
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args == nil || reply == nil {
		panic("RequestVote: params can't be nil")
	}
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.curState = follower
	}
	if rf.curState != follower {
		Info(rf.me, "reject request vote for term %v from %v due to not follower", args.Term, args.CandidateId)
		reply.VoteGranted = false
		return
	}
	if args.Term < rf.currentTerm {
		Info(rf.me, "reject request vote for term %v from %v due to term", args.Term, args.CandidateId)
		reply.VoteGranted = false
		return
	}
	if _, ok := rf.votedFor[args.Term]; ok {
		Info(rf.me, "reject request vote for term %v from %v due to already voted", args.Term, args.CandidateId)
		reply.VoteGranted = false
		return
	}
	if len(rf.logs) != 0 && rf.logs[rf.commitIndex].term > args.LastLogTerm {
		Info(rf.me, "reject request vote for term %v from %v due to commit version", args.Term, args.CandidateId)
		reply.VoteGranted = false
		return
	}
	if len(rf.logs) != 0 && rf.logs[rf.commitIndex].term == args.LastLogIndex &&
		rf.logs[rf.commitIndex].index > args.LastLogIndex {
		Info(rf.me, "reject request vote for term %v from %v due to commit version", args.Term, args.CandidateId)
		reply.VoteGranted = false
		return
	}

	rf.votedFor[args.Term] = args.CandidateId
	Info(rf.me, "grant request vote for term %v from %v", args.Term, args.CandidateId)
	reply.VoteGranted = true
}

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderID     int // leader's id
	PrevLogIndex int // previous log entry index preceding this new one
	PrevLogTerm  int // previous log entry term
	LeaderCommit int // leader's commit index
}

type AppendEntriesReply struct {
	Term    int  // current term
	Success bool // true if follower contained entry matching PrevLogTerm and PrevLogIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	rf.lastHeartBeat = time.Now()

	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	}
	if len(rf.logs) != 0 && rf.logs[len(rf.logs)-1].term < args.PrevLogTerm {
		reply.Success = false
		return
	}
	if len(rf.logs) != 0 && rf.logs[len(rf.logs)-1].term == args.PrevLogTerm &&
		rf.logs[len(rf.logs)-1].index < args.PrevLogIndex {
		reply.Success = false
		return
	}

	// delete unmatched entries

	// append

	// update commit index

	// update current term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.curState = follower
	}

	reply.Success = true
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// pause for a random amount of time between 50 and 1050
		// milliseconds.
		ms := 50 + (rand.Int63() % 1000)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.curState == follower && time.Now().Sub(rf.lastHeartBeat).Milliseconds() > 500 {
			// start a new election
			rf.currentTerm += 1
			rf.curState = candidate
			rf.votedFor[rf.currentTerm] = rf.me
			rf.persist()
			lastLogTerm := 0
			lastLogIndex := 0
			if len(rf.logs) != 0 {
				lastLogTerm = rf.logs[rf.commitIndex].term
				lastLogIndex = rf.logs[rf.commitIndex].index
			}
			Info(rf.me, "Term %v, voted for myself", rf.currentTerm)
			go rf.elect(rf.currentTerm, rf.me, lastLogTerm, lastLogIndex, rf.peers)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) elect(currentTerm, candidateId, lastLogTerm, lastLogIndex int, peers []*labrpc.ClientEnd) {
	electionTimeout := 1500 * time.Millisecond // election timeout is 1.5s

	getVote := 1
	voteChan := make(chan struct{}, len(peers))
	for _, c := range peers {
		go func(client *labrpc.ClientEnd) {
			reply := &RequestVoteReply{}
			ok := client.Call("Raft.RequestVote", &RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  candidateId,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}, &reply)

			if ok && reply.VoteGranted {
				voteChan <- struct{}{}
			}
		}(c)
	}

	timer := time.NewTicker(electionTimeout)
	defer timer.Stop()

	for {
		select {
		case <-voteChan:
			getVote += 1
			if getVote > len(peers)/2 {
				// voted as leader
				rf.mu.Lock()
				if rf.curState != candidate {
					panic("Error occur! When elected, it's not candidate state")
				}
				rf.curState = leader
				rf.mu.Unlock()
				return
			}
		case <-timer.C:
			// timeout
			rf.mu.Lock()
			Info(rf.me, "election timeout")
			rf.mu.Unlock()
			goto timeout
		}
	}

timeout:
	// if executed to here, than election timeout, need a new election
	rf.mu.Lock()
	if rf.curState != candidate {
		// there is another leader
		Info(rf.me, "There is another leader. Exit from election")
		rf.mu.Unlock()
		return
	}
	// if rf.votedFor[rf.currentTerm] != rf.me {
	// 	panic("Error occured! Candidate's votedFor is not itself")
	// }

	// sleep for a random time
	rf.curState = follower
	rf.mu.Unlock()
	ms := 100 + (rand.Int63() % 1000)
	time.Sleep(time.Duration(ms) * time.Millisecond)
	rf.mu.Lock()
	rf.curState = candidate

	rf.currentTerm += 1
	rf.persist()
	lastLogTerm = 0
	lastLogIndex = 0
	if len(rf.logs) != 0 {
		lastLogTerm = rf.logs[rf.commitIndex].term
		lastLogIndex = rf.logs[rf.commitIndex].index
	}
	rf.votedFor[rf.currentTerm] = rf.me
	Info(rf.me, "Term %v, voted for myself", rf.currentTerm)
	go rf.elect(rf.currentTerm, rf.me, lastLogTerm, lastLogIndex, rf.peers)
	rf.mu.Unlock()
}

func (rf *Raft) heartBeat() {
	for {
		if rf.killed() {
			break
		}
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
		if rf.curState != leader {
			rf.mu.Unlock()
			continue
		}
		curTerm := rf.currentTerm
		id := rf.me
		prevLogIndex := 0
		prevLogTerm := 0
		if len(rf.logs) != 0 {
			prevLog := rf.logs[len(rf.logs)-1]
			prevLogIndex = prevLog.index
			prevLogTerm = prevLog.term
		}
		commit := rf.commitIndex
		peers := rf.peers
		rf.mu.Unlock()

		for _, c := range peers {
			go func(client *labrpc.ClientEnd) {
				reply := &AppendEntriesReply{}
				client.Call("Raft.AppendEntries", &AppendEntriesArgs{
					Term:         curTerm,
					LeaderID:     id,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					LeaderCommit: commit,
				}, &reply)
			}(c)
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	Info(me, "created")
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.curState = follower

	// Your initialization code here (2A, 2B, 2C).
	rf.applyChan = applyCh
	rf.votedFor = make(map[int]int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartBeat()

	return rf
}
