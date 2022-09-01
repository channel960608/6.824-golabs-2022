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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
const (
	HEARTBEAT_INTERVAL = 100
	FOLLOWER           = 0
	CANDIDATE          = 1
	LEADER             = 2
)

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
	role int

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	rand              *rand.Rand
	heartbeatInterval int
	electionTimeout   int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) randomElectionTimeout() {
	rf.fmt("Reset the election timeout")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimeout = 200 + int(rf.rand.Int31n(300))
}

func (rf *Raft) fmt(a ...interface{}) {
	fmt.Println("Node", rf.me, "Term", rf.currentTerm, "votedFor", rf.votedFor, "commitIndex", rf.commitIndex, ":", a)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	// voteChan := make(chan int, len(rf.peers))
	// args := AppendEntriesArgs{}
	// args.Term = rf.currentTerm
	// args.LeaderId = rf.me
	// for i := 0; i < len(rf.peers); i += 1 {
	// 	if i != rf.me {
	// 		go func(i int) {
	// 			reply := AppendEntriesReply{}
	// 			ok := rf.sendAppendEntries(i, &args, &reply)
	// 			if ok {
	// 				if reply.Success {
	// 					voteChan <- 0
	// 				}
	// 			}
	// 		}(i)
	// 	}
	// }
	// time.Sleep(time.Millisecond * time.Duration(50))
	// close(voteChan)
	// isleader = !rf.killed() && rf.votedFor == rf.me && len(voteChan)+1 > len(rf.peers)/2
	isleader = !rf.killed() && rf.role == LEADER && rf.votedFor == rf.me
	term = rf.currentTerm
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
}

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// rf.randomElectionTimeout()

	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votedFor = -1
		rf.mu.Unlock()
		rf.randomElectionTimeout()
	}

	rf.fmt("Receive RequestVote Request from Node", args.CandidateId, "Term", args.Term)

	reply.VoteGranted = false

	if rf.currentTerm > args.Term {
		return
	}

	rf.fmt("State before vote:", "rf.votedFor", rf.votedFor, "args.CandidateId", args.CandidateId, "args.LastLogTerm", args.LastLogTerm, "args.LastLogIndex", args.LastLogIndex, "len(rf.log)", len(rf.log))
	rf.fmt(rf.votedFor == -1 || rf.votedFor == args.CandidateId, args.LastLogIndex >= len(rf.log))
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.LastLogIndex >= len(rf.log) {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.mu.Lock()
		rf.votedFor = args.CandidateId
		rf.mu.Unlock()
		rf.randomElectionTimeout()
	}
	rf.fmt("Vote ", reply.VoteGranted, "for Node", args.CandidateId)
	return
}

//
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = FOLLOWER
		rf.mu.Unlock()
		rf.randomElectionTimeout()
	}

	rf.fmt("Receive AppendEntries Request from Node", args.LeaderId, "Term", args.Term)

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	rf.randomElectionTimeout()

	entries_length := len(rf.log)

	if entries_length < args.PrevLogIndex || args.PrevLogIndex-1 >= 0 && rf.log[args.PrevLogIndex-1].Term != args.PreLogTerm {
		reply.Success = false
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = args.LeaderId
	// Find the matched
	for index := 0; index < len(args.Entries); index += 1 {
		if args.PrevLogIndex+index < len(rf.log) {
			if rf.log[args.PrevLogIndex+index].Term != args.Entries[index].Term {
				// conflic
				rf.fmt("conflict")
				logCopy := make([]LogEntry, args.PrevLogIndex)
				copy(logCopy, rf.log[0:args.PrevLogIndex])
				logCopy = append(logCopy, args.Entries...)
				rf.log = logCopy
			}
		} else {
			rf.log = append(rf.log, args.Entries[index:]...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = len(rf.log)
		if args.LeaderCommit < rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
		}
	}

	reply.Success = true
	return
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

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.fmt("Killed")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() bool {

	voteChannel := make(chan int, len(rf.peers)+1)

	rf.mu.Lock()
	rf.role = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	voteChannel <- rf.currentTerm
	rf.mu.Unlock()

	rf.randomElectionTimeout()

	timeout := 50
	rf.fmt("Start a new Election with timeout", timeout)

	// voteChannel <- rf.currentTerm
	for i := 0; i < len(rf.peers) && !rf.killed(); i += 1 {
		go func(i int) {
			if i != rf.me {
				args := RequestVoteArgs{}
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				args.LastLogIndex = len(rf.log)
				if args.LastLogIndex == 0 {
					args.LastLogTerm = 0
				} else {
					args.LastLogTerm = rf.log[args.LastLogIndex-1].Term
				}
				reply := RequestVoteReply{}
				rf.fmt("Send vote Requset to Node", i)
				ok := rf.sendRequestVote(i, &args, &reply)
				if ok {
					rf.fmt("Receive reply from Node", i)
					if reply.VoteGranted {
						voteChannel <- reply.Term
					} else {
						rf.fmt("Not receive true from Node", i)
					}
					// Become follower
					if reply.Term > rf.currentTerm {
						rf.mu.Lock()
						rf.currentTerm = args.Term
						rf.role = FOLLOWER
						rf.votedFor = -1
						rf.mu.Unlock()
					}
				} else {
					rf.fmt("Error to receive reply of RequestVote from Node", i)
				}
			}
		}(i)
	}

	rf.fmt("Sleep", timeout, "ms for Election")
	time.Sleep(time.Millisecond * time.Duration(timeout))
	close(voteChannel)
	rf.fmt("Current election timeout")
	voteCount := 0
	for !rf.killed() && rf.votedFor == rf.me {
		voteTerm, ok := <-voteChannel
		if ok {
			voteCount += 1
			rf.fmt("Result of the Election, vote as Leader of Term ", voteTerm, ", vote count now is", voteCount)
		} else {
			break
		}
	}
	// close(voteChannel)

	result := voteCount > len(rf.peers)/2
	rf.fmt("The Election result is", result)

	return result
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// Check heartbeat count during
		lastElectionTimeout := rf.electionTimeout
		rf.fmt("Sleep", lastElectionTimeout, "ms")
		time.Sleep(time.Duration(lastElectionTimeout) * time.Millisecond)

		for rf.killed() == false && lastElectionTimeout == rf.electionTimeout {
			// Become Candidate
			// Start Election

			rf.fmt("HeartBeat timeout, becomes Candidate!")
			electionResult := rf.startElection()
			if electionResult {
				rf.fmt("Win the election, becomes Leader")

				rf.mu.Lock()
				// reinitialize leader's volatile state
				rf.role = LEADER
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i += 1 {
					rf.nextIndex[i] = 1 + len(rf.peers)
				}
				rf.mu.Unlock()

				for !rf.killed() {
					// heartBeatCh := make(chan bool, len(rf.peers)+1)
					// defer close(heartBeatCh)
					args := AppendEntriesArgs{}
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					// Send HeartBeat
					for i := 0; i < len(rf.peers); i += 1 {
						if i != rf.me {
							rf.fmt("Send HeartBeart to Node", i)
							go func(i int) {
								reply := AppendEntriesReply{}
								ok := rf.sendAppendEntries(i, &args, &reply)
								if ok {
									if reply.Term > rf.currentTerm {
										rf.fmt("Becomes follower from Leader Node", i)
										rf.mu.Lock()
										rf.currentTerm = args.Term
										rf.mu.Unlock()
									}
								}
							}(i)
						}
					}

					time.Sleep(time.Duration(rf.heartbeatInterval) * time.Millisecond)

					if rf.killed() {
						rf.fmt("Get Killed")
						rf.fmt("Stop sending HearBeats")
						break
					}
					if lastElectionTimeout != rf.electionTimeout || rf.votedFor != rf.me {
						rf.fmt("Stop sending HearBeats")
						break
					}
				}
			}

		}

	}
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

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.role = FOLLOWER

	rf.rand = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	rf.heartbeatInterval = HEARTBEAT_INTERVAL

	// rf.randomElectionTimeout()
	rf.fmt("Make a newborn")

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
