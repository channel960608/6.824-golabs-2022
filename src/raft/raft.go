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
	"sort"
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
	HEARTBEAT_INTERVAL    = 100
	RPC_timeout           = 50
	FOLLOWER              = 0
	CANDIDATE             = 1
	LEADER                = 2
	LIMITED_ENTRIES_COUNT = 5
)

func getRoleName(code int) string {
	if code == 0 {
		return "FOLLOWER"
	} else if code == 1 {
		return "CANDIDATE"
	} else if code == 2 {
		return "LEADER"
	} else {
		return "UNKNOWN"
	}
}

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

	applyCh chan ApplyMsg
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
	preRole := rf.role
	if preRole != role {
		rf.role = role
		rf.logger(logger.DInfo, "Role change: ", getRoleName(preRole), " -> ", getRoleName(role))
	}
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

func (rf *Raft) appendLogEntriesAt(start int, entries []LogEntry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = append(rf.log[:start], entries...)
}

func (rf *Raft) appendLogEntries(entries []LogEntry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = append(rf.log, entries...)
}

func (rf *Raft) removeFollowingLogEntries(start int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = rf.log[:start]
}

func (rf *Raft) getNextIndex(index int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex[index]
}

func (rf *Raft) getNextIndexAll() []int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex
}

func (rf *Raft) setNextIndex(index int, value int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	preValue := rf.nextIndex[index]
	if preValue != value {
		rf.nextIndex[index] = value
		rf.logger(logger.DLog, "nextIndex[", index, "] ", preValue, " -> ", value)
	}
}

func (rf *Raft) getMatchIndexAll() []int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.matchIndex
}

func (rf *Raft) getMatchIndex(index int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.matchIndex[index]
}

func (rf *Raft) setMatchIndex(index int, value int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	preValue := rf.matchIndex[index]
	if preValue > value {
		rf.logger(logger.DError, "Fail to change MatchIndex[", index, "]: ", preValue, " -> ", value, "MatchIndex is monotonical!")
	}
	if preValue != value {
		rf.matchIndex[index] = value
		rf.logger(logger.DLog, "matchIndex[", index, "] ", preValue, " -> ", value)
	}
}

func (rf *Raft) setCommitIndex(commitIndex int) {
	var preCommitIndex int
	rf.mu.Lock()
	defer rf.mu.Unlock()
	preCommitIndex = rf.commitIndex
	if preCommitIndex > commitIndex {
		rf.logger(logger.DError, "Fail to change CommitIndex ", preCommitIndex, " -> ", commitIndex, "CommitIndex is monotonical!")
	}
	if preCommitIndex != commitIndex {
		rf.commitIndex = commitIndex
		rf.logger(logger.DCommit, "CommitIndex ", preCommitIndex, " -> ", commitIndex)

		for i := preCommitIndex + 1; i <= rf.commitIndex; i += 1 {
			am := ApplyMsg{}
			am.CommandValid = true
			am.CommandIndex = i
			am.Command = rf.log[i-1].Command
			rf.applyCh <- am
			rf.logger(logger.DCommit, "Send ApplyMsg to applyCh for command index ", i, " Command = ", am.Command)
		}
	}
}

func (rf *Raft) randomElectiontimeout() {
	rf.setElectiontimeout(200 + int(rf.rand.Int31n(300)))
	rf.logger(logger.DTimer, "Reset the election timeout = ", rf.getElectiontimeout())
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

	if (vf == -1 || vf == args.CandidateId) && args.LastLogIndex >= rf.getCommitIndex() {
		reply.VoteGranted = true
		reply.Term = rf.getCurrentTerm()
		rf.setVotedFor(args.CandidateId)
		rf.randomElectiontimeout()
	}
	rf.logger(logger.DVote, "Vote ", reply.VoteGranted, " for N", args.CandidateId, " status: vf=", vf, "  args.LastLogIndex=", args.LastLogIndex, " ")
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
		rf.logger(logger.DWarn, "Receive the AppendEntries Request args.Term > rf.getCurrentTerm()")
		rf.setCurrentTerm(args.Term)
		rf.setVotedFor(-1)
		rf.setRole(FOLLOWER)
		rf.randomElectiontimeout()
	}

	rf.logger(logger.DClient, "Receive AppendEntries Request from N", args.LeaderId, " T", args.Term)

	reply.Term = rf.getCurrentTerm()
	reply.Success = false

	if args.Term < rf.getCurrentTerm() {
		rf.logger(logger.DError, "Refuse the AppendEntries Request args.Term < rf.getCurrentTerm()")
		return
	}

	rf.randomElectiontimeout()

	entries_length := len(rf.getLog())

	if entries_length < args.PrevLogIndex || args.PrevLogIndex >= 1 && rf.getLog()[args.PrevLogIndex-1].Term != args.PreLogTerm {
		rf.logger(logger.DError, "Doesn't contain an entry at prevLogIndex ", args.PrevLogIndex, " whose term matches prevLogTerm")
		reply.Success = false
		return
	}

	rf.setVotedFor(args.LeaderId)
	// Find the matched
	rf.logger(logger.DLog, "New comming log entries start at index ", args.PrevLogIndex+1, ":", args.Entries)
	for index := 0; index < len(args.Entries); index += 1 {
		if args.PrevLogIndex+index < len(rf.log) {
			if rf.log[args.PrevLogIndex+index].Term != args.Entries[index].Term {
				// conflic
				rf.logger(logger.DWarn, "Log Entry Conflict happens at index ", args.PrevLogIndex+index+1, ", remove the entries start from this place.")
				rf.appendLogEntriesAt(args.PrevLogIndex+index, args.Entries[index:])
				break
			}
		} else {
			rf.logger(logger.DLog, "Append log entries: ", args.Entries[index:])
			rf.appendLogEntries(args.Entries[index:])
			break
		}
	}
	if args.LeaderCommit > rf.getCommitIndex() {
		rf.logger(logger.DLog, "args.LeaderCommit > rf.getCommitIndex()")
		ci := len(rf.getLog())
		if args.LeaderCommit < ci {
			ci = args.LeaderCommit
		}
		rf.setCommitIndex(ci)
	}
	// rf.logger(logger.DClient, "Current entries: ", rf.getLog(), " rf.commitIndex()=", rf.getCommitIndex())
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
	// Your code here (2B).
	isLeader := true
	if rf.getRole() != LEADER {
		isLeader = false
	} else {
		entries := make([]LogEntry, 1)
		entries[0] = LogEntry{rf.getCurrentTerm(), command}
		rf.appendLogEntries(entries)
		index = len(rf.getLog())
		term = rf.getCurrentTerm()
	}
	rf.logger(logger.DLog2, "index = ", index, " term = ", term, " isLeader = ", isLeader)
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

	// voteChannel <- rf.currentTerm
	for i := 0; i < len(rf.peers) && !rf.killed(); i += 1 {
		go func(i int) {
			if i != rf.me {
				args := RequestVoteArgs{}
				args.Term = rf.getCurrentTerm()
				args.CandidateId = rf.me
				args.LastLogIndex = rf.getCommitIndex()
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
			rf.randomElectiontimeout()
			rf.logger(logger.DVote, "Start a new Election with timeout ", RPC_timeout, ", update lastElectiontimeout")
			lastElectiontimeout = rf.getElectiontimeout()
			electionResult := rf.startElection()
			if electionResult {
				rf.logger(logger.DLeader, "Send heartbeats to all other peers immediately.")
				for i := 0; i < len(rf.peers); i += 1 {
					go func(i int) {
						if i != rf.me {
							args := AppendEntriesArgs{}
							args.Term = rf.getCurrentTerm()
							args.LeaderId = rf.me
							reply := AppendEntriesReply{}
							rf.sendAppendEntries(i, &args, &reply)
						}
					}(i)
				}
				rf.setRole(LEADER)
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i += 1 {
					rf.setNextIndex(i, 1+len(rf.getLog()))
				}

				for !rf.killed() && rf.getRole() == LEADER {
					// check matchIndex, find the N, N > commitIndex && N <= majority of matchIndex
					matchIndexArray := make([]int, len(rf.getMatchIndexAll()))
					copy(matchIndexArray, rf.getMatchIndexAll())
					sort.Slice(matchIndexArray, func(i, j int) bool {
						return matchIndexArray[i] < matchIndexArray[j]
					})
					N := matchIndexArray[len(matchIndexArray)/2]
					if N > rf.getCommitIndex() {
						rf.logger(logger.DCommit, "Found an N > commitIndex && N <= majority of matchIndex")
						rf.setCommitIndex(N)
					}

					rf.logger(logger.DLeader, "Current matchIndex: ", rf.getMatchIndexAll(), " commitIndex: ", rf.getCommitIndex(), " nextIndex: ", rf.getNextIndexAll(), " len(rf.log): ", len(rf.getLog()))

					for i := 0; i < len(rf.peers); i += 1 {
						go func(i int) {
							if i == rf.me {
								rf.setMatchIndex(i, len(rf.getLog()))
								rf.setNextIndex(i, len(rf.getLog())+1)
								return
							}
							rf.logger(logger.DLeader, "rf.getNextIndex(i)=", rf.getNextIndex(i), " ,len(rf.getLog())=", len(rf.getLog()))

							for !rf.killed() && rf.getRole() == LEADER {
								args := AppendEntriesArgs{}
								args.Term = rf.getCurrentTerm()
								args.LeaderId = rf.me
								args.LeaderCommit = rf.getCommitIndex()
								args.PrevLogIndex = rf.getNextIndex(i) - 1
								if args.PrevLogIndex-1 >= 0 {
									args.PreLogTerm = rf.getLog()[args.PrevLogIndex-1].Term
								} else {
									args.PreLogTerm = 0
								}
								// limit the size of Entires to up to 5
								rightIndex := args.PrevLogIndex + LIMITED_ENTRIES_COUNT
								if rightIndex > len(rf.getLog()) {
									rightIndex = len(rf.getLog())
								}

								args.Entries = rf.getLog()[args.PrevLogIndex:rightIndex]
								// args.Entries = rf.getLog()[rf.getNextIndex(i)-1:]

								rf.logger(logger.DLeader, "Send AppendEntries to N", i, " len(Entries)= ", len(args.Entries))
								reply := AppendEntriesReply{}
								ok := rf.sendAppendEntries(i, &args, &reply)
								rf.logger(logger.DTest, "Send Entry at Index: ", args.PrevLogIndex, " to N", i, " len(entries)=", len(args.Entries))
								if ok {
									rf.logger(logger.DLeader, "Receive reply from N", i)
									if reply.Term > rf.getCurrentTerm() {
										rf.logger(logger.DWarn, "Becomes follower of Leader N", i)
										rf.setRole(FOLLOWER)
										rf.setCurrentTerm(args.Term)
										break
									}
									if reply.Success {
										rf.logger(logger.DLeader, "Update matchIndex, args.PrevLogIndex=", args.PrevLogIndex, ",  len(args.Entries)=", len(args.Entries))
										rf.setMatchIndex(i, args.PrevLogIndex+len(args.Entries))
										rf.setNextIndex(i, rf.getMatchIndex(i)+1)
										break
									} else {
										rf.setNextIndex(i, rf.getNextIndex(i)-1)
										rf.logger(logger.DLog, "Fail to replicate log entries, decrease nextIndex[", i, "] , then retry")
										if rf.getNextIndex(i) <= rf.getCommitIndex() {
											rf.logger(logger.DError, "nextIndex[", i, "] <= commited index ", rf.getCommitIndex(), "error condition! Fail to replicate entries.")
											break
										}
									}

								} else {
									rf.logger("Fail to receive reply from N", i)
								}
							}
						}(i)
					}

					time.Sleep(time.Duration(rf.heartbeatInterval) * time.Millisecond)

					if rf.killed() {
						rf.logger(logger.DWarn, "Get Killed, stop sending HearBeats")
						break
					}
					if rf.getElectiontimeout() != lastElectiontimeout || rf.getVotedFor() != rf.me {
						if rf.getElectiontimeout() != lastElectiontimeout {
							rf.logger(logger.DError, "Condition Error: rf.getElectiontimeout() != lastElectiontimeout is False, rf.getElectiontimeout() = ", rf.getElectiontimeout(), ", lastElectiontimeout = ", lastElectiontimeout)
						} else if rf.getVotedFor() != rf.me {
							rf.logger(logger.DError, "Condition Error: rf.getVotedFor() != rf.me is False, rf.getVotedFor() = ", rf.getVotedFor(), ", rf.me = ", rf.me)
						}

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

	rf.applyCh = applyCh
	// rf.randomElectiontimeout()
	rf.logger(logger.DInfo, "Make a newborn")

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
