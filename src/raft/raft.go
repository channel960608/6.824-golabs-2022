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
	"6.824/logger"
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
	RPC_timeout        = 50
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
	electiontimeout   int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) getRole() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

func (rf *Raft) setRole(role int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = role
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) setCurrentTerm(ct int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = ct
}

func (rf *Raft) getVotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}

func (rf *Raft) setVotedFor(target int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = target
}

func (rf *Raft) getLog() []LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log
}

func (rf *Raft) getElectiontimeout() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.electiontimeout
}

func (rf *Raft) setElectiontimeout(timeout int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electiontimeout = timeout
}

func (rf *Raft) getCommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

func (rf *Raft) setCommitIndex(commitIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = commitIndex
}

func (rf *Raft) randomElectiontimeout() {
	rf.logger(logger.DTimer, "Reset the election timeout")
	rf.setElectiontimeout(200 + int(rf.rand.Int31n(300)))
}

func (rf *Raft) logger(topic logger.LogTopic, a ...interface{}) {
	// content := fmt.Printf("Node", rf.me, "Term", rf.getCurrentTerm(), "votedFor", rf.getVotedFor(), "commitIndex", rf.getCommitIndex(), ":", a)
	// pre := fmt.Sprintf("N:%dT:%dV:%d: ", rf.me, rf.getCurrentTerm(), rf.getVotedFor())
	pre := fmt.Sprintf("N%v ", rf.me)
	content := fmt.Sprint(a...)
	logger.Debug(topic, pre+content)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	isleader = !rf.killed() && rf.getRole() == LEADER && rf.getVotedFor() == rf.me
	term = rf.getCurrentTerm()
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
	if args.Term > rf.getCurrentTerm() {
		rf.setCurrentTerm(args.Term)
		rf.setRole(FOLLOWER)
		rf.setVotedFor(-1)
	}
	rf.logger(logger.DVote, "Receive RequestVote Request from N", args.CandidateId, "T", args.Term)

	reply.VoteGranted = false

	if rf.getCurrentTerm() > args.Term {
		return
	}
	vf := rf.getVotedFor()

	if (vf == -1 || vf == args.CandidateId) && args.LastLogIndex >= len(rf.getLog()) {
		reply.VoteGranted = true
		reply.Term = rf.getCurrentTerm()
		rf.setVotedFor(args.CandidateId)
		rf.randomElectiontimeout()
	}
	rf.logger(logger.DVote, "Vote ", reply.VoteGranted, "for N", args.CandidateId)
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
	if args.Term > rf.getCurrentTerm() {
		rf.setCurrentTerm(args.Term)
		rf.setVotedFor(-1)
		rf.setRole(FOLLOWER)
		rf.randomElectiontimeout()
	}

	rf.logger(logger.DClient, "Receive AppendEntries Request from N", args.LeaderId, " T", args.Term)

	reply.Term = rf.getCurrentTerm()
	reply.Success = false

	if args.Term < rf.getCurrentTerm() {
		return
	}

	rf.randomElectiontimeout()

	entries_length := len(rf.getLog())

	if entries_length < args.PrevLogIndex || args.PrevLogIndex-1 >= 0 && rf.getLog()[args.PrevLogIndex-1].Term != args.PreLogTerm {
		reply.Success = false
		return
	}

	rf.setVotedFor(args.LeaderId)
	// Find the matched
	rf.mu.Lock()
	for index := 0; index < len(args.Entries); index += 1 {
		if args.PrevLogIndex+index < len(rf.log) {
			if rf.log[args.PrevLogIndex+index].Term != args.Entries[index].Term {
				// conflic
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
	rf.mu.Unlock()
	if args.LeaderCommit > rf.getCommitIndex() {
		rf.setCommitIndex(len(rf.getLog()))
		if args.LeaderCommit < rf.getCommitIndex() {
			rf.setCommitIndex(args.LeaderCommit)
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
	rf.logger(logger.DInfo, "Killed")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() bool {

	voteChannel := make(chan int, len(rf.peers)+1)
	defer close(voteChannel)
	rf.setRole(CANDIDATE)
	rf.setCurrentTerm(rf.getCurrentTerm() + 1)
	rf.setVotedFor(rf.me)
	voteChannel <- rf.currentTerm

	rf.randomElectiontimeout()

	rf.logger(logger.DVote, "Start a new Election with timeout", RPC_timeout)

	// voteChannel <- rf.currentTerm
	for i := 0; i < len(rf.peers) && !rf.killed(); i += 1 {
		go func(i int) {
			if i != rf.me {
				args := RequestVoteArgs{}
				args.Term = rf.getCurrentTerm()
				args.CandidateId = rf.me
				args.LastLogIndex = len(rf.getLog())
				if args.LastLogIndex == 0 {
					args.LastLogTerm = 0
				} else {
					args.LastLogTerm = rf.getLog()[args.LastLogIndex-1].Term
				}
				reply := RequestVoteReply{}
				rf.logger(logger.DVote, "Send vote Requset to N", i)
				ok := rf.sendRequestVote(i, &args, &reply)
				if ok {
					rf.logger(logger.DVote, "Receive reply from N", i)
					if reply.VoteGranted {
						voteChannel <- reply.Term
					}
					// Become follower
					if reply.Term > rf.getCurrentTerm() {
						rf.setCurrentTerm(args.Term)
						rf.setRole(FOLLOWER)
						rf.setVotedFor(-1)
					}

				} else {
					rf.logger(logger.DVote, "Error to receive reply of RequestVote from N", i)
				}
			}
		}(i)
	}

	rf.logger(logger.DVote, "Sleep ", RPC_timeout, "ms for Election")
	time.Sleep(time.Millisecond * time.Duration(RPC_timeout))
	voteChannel <- -1
	rf.logger(logger.DVote, "Current election timeout")
	voteCount := 0
	for !rf.killed() && rf.getVotedFor() == rf.me {
		voteTerm, ok := <-voteChannel
		if ok && voteTerm >= 0 {
			voteCount += 1
			rf.logger(logger.DVote, "Result of the Election, vote as Leader of T", voteTerm, ", vote count now is ", voteCount)
		} else {
			break
		}
	}

	result := voteCount > len(rf.peers)/2
	rf.logger(logger.DVote, "The Election result is ", result)

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
		lastElectiontimeout := rf.getElectiontimeout()
		rf.logger(logger.DTimer, "Sleep ", lastElectiontimeout, "ms")
		time.Sleep(time.Duration(lastElectiontimeout) * time.Millisecond)

		for rf.killed() == false && rf.getElectiontimeout() == lastElectiontimeout {
			// Become Candidate
			// Start Election
			rf.logger(logger.DWarn, "HeartBeat timeout, becomes Candidate!")
			electionResult := rf.startElection()
			if electionResult {
				rf.logger(logger.DLeader, "Win the election, becomes Leader")

				// reinitialize leader's volatile state
				rf.setRole(LEADER)
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i += 1 {
					rf.nextIndex[i] = 1 + len(rf.peers)
				}

				for !rf.killed() {
					// heartBeatCh := make(chan bool, len(rf.peers)+1)
					// defer close(heartBeatCh)
					args := AppendEntriesArgs{}
					args.Term = rf.getCurrentTerm()
					args.LeaderId = rf.me
					// Send HeartBeat
					for i := 0; i < len(rf.peers); i += 1 {
						if i != rf.me {
							rf.logger(logger.DLeader, "Send HeartBeart to N", i)
							go func(i int) {
								reply := AppendEntriesReply{}
								ok := rf.sendAppendEntries(i, &args, &reply)
								if ok {
									if reply.Term > rf.getCurrentTerm() {
										rf.logger(logger.DWarn, "Becomes follower of Leader N", i)
										rf.setCurrentTerm(args.Term)
									}
								}
							}(i)
						}
					}

					time.Sleep(time.Duration(rf.heartbeatInterval) * time.Millisecond)

					if rf.killed() {
						rf.logger(logger.DWarn, "Get Killed, stop sending HearBeats")
						break
					}
					if rf.getElectiontimeout() != lastElectiontimeout || rf.getVotedFor() != rf.me {
						rf.logger(logger.DWarn, "Stop sending HearBeats")
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

	// rf.randomElectiontimeout()
	rf.logger(logger.DInfo, "Make a newborn")

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
