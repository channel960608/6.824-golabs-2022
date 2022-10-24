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
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
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

	applyCh        chan ApplyMsg
	lastApplyIndex int
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
	rf.persist()
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
	rf.persist()
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

func (rf *Raft) appendLogEntriesAt(start int, entries []LogEntry) (int, int) {
	preEntries := make([]LogEntry, start)
	copy(preEntries, rf.log[:start])
	rf.log = append(preEntries, entries...)
	term := 0
	rf.persist()
	if len(rf.log) > 0 {
		term = rf.log[len(rf.log)-1].Term
	}
	rf.persist()
	return len(rf.log), term
}

func (rf *Raft) appendLogEntriesAtWithLock(start int, entries []LogEntry) (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.appendLogEntriesAt(start, entries)
}

func (rf *Raft) appendLogEntries(entries []LogEntry) (int, int) {
	rf.logger(logger.DLog, "Append Log Entries: ", rf.log, " + ", entries)
	rf.log = append(rf.log, entries...)
	rf.persist()
	term := 0
	if len(rf.log) > 0 {
		term = rf.log[len(rf.log)-1].Term
	}
	return len(rf.log), term
}

func (rf *Raft) appendLogEntriesWithLock(entries []LogEntry) (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.appendLogEntries(entries)
}

func (rf *Raft) removeFollowingLogEntries(start int) (int, int) {
	rf.log = rf.log[:start]
	rf.persist()
	term := 0
	if len(rf.log) > 0 {
		term = rf.log[len(rf.log)-1].Term
	}
	return len(rf.log), term
}

func (rf *Raft) removeFollowingLogEntriesWithLock(start int) (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.removeFollowingLogEntries(start)
}

func (rf *Raft) getNextIndex(index int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex[index]
}

func (rf *Raft) getNextIndexAll() []int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	cpNextIndex := make([]int, len(rf.nextIndex))
	copy(cpNextIndex, rf.nextIndex)
	return cpNextIndex
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
	cpMatchIndex := make([]int, len(rf.matchIndex))
	copy(cpMatchIndex, rf.matchIndex)
	return cpMatchIndex
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
	} else if preCommitIndex != commitIndex {
		rf.commitIndex = commitIndex
		rf.logger(logger.DCommit, "CommitIndex ", preCommitIndex, " -> ", commitIndex)
	}
}

func (rf *Raft) getLastApplyIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastApplyIndex
}

func (rf *Raft) setLastApplyIndex(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	pre := rf.lastApplyIndex
	if pre < index {
		rf.lastApplyIndex = index
		rf.persist()
	} else {
		rf.logger("Fail to update lastApplyIndex ", pre, " -> ", index, ", it is monotonical!")
	}

}

func (rf *Raft) syncApplyCh() {
	for !rf.killed() {
		commitIndex := rf.getCommitIndex()
		lastAppliedIndex := rf.getLastApplyIndex()
		preSyncedIndex := lastAppliedIndex
		if commitIndex > lastAppliedIndex {
			for v := lastAppliedIndex + 1; v <= commitIndex; v += 1 {
				am := ApplyMsg{}
				am.CommandValid = true
				am.CommandIndex = v
				log := rf.getLog()
				if v > len(log) {
					rf.logger(logger.DError, "Break when v >= len(rf.log), log: ", log, " commitIndex=", commitIndex)
					break
				}
				am.Command = log[v-1].Command
				rf.applyCh <- am
				rf.logger(logger.DCommit, "Send ApplyMsg to applyCh for command index ", am.CommandIndex, " Command = ", am.Command)
				preSyncedIndex = v
			}
			rf.setLastApplyIndex(preSyncedIndex)
		}
		time.Sleep(time.Millisecond * time.Duration(RPC_timeout))
	}
}

func (rf *Raft) randomElectiontimeout() {
	var nextRandomValue int
	rf.mu.Lock()
	nextRandomValue = 300 + int(rf.rand.Int31n(300))
	rf.mu.Unlock()
	rf.setElectiontimeout(nextRandomValue)
	rf.logger(logger.DTimer, "Reset the election timeout = ", rf.getElectiontimeout())
}

func (rf *Raft) logger(topic logger.LogTopic, a ...interface{}) {
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastApplied)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	rf.logger(logger.DPersist, "Persist current state.")
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastApplied int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastApplied) != nil {
		rf.logger(logger.DError, "Error when read and decode bytes form persist data.")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastApplied = lastApplied
		rf.logger(logger.DPersist, "Read from persist: currentTerm=", currentTerm, ", votedFor=", votedFor, " lastApplied=", lastApplied, ", log size: ", len(log))
	}
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
	lastIndex := len(rf.getLog())
	lastTerm := 0
	if lastIndex != 0 {
		lastTerm = rf.getLog()[lastIndex-1].Term
	}
	// The definition of more up-to-date log: the index of the last entry is larger || the index of the last entry is the same but have larger term
	if (vf == -1 || vf == args.CandidateId) && (args.LastLogTerm > lastTerm || args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex) {
		reply.VoteGranted = true
		reply.Term = rf.getCurrentTerm()
		rf.setVotedFor(args.CandidateId)
		rf.randomElectiontimeout()
	}
	rf.logger(logger.DVote, "Vote ", reply.VoteGranted, " for N", args.CandidateId, " status: vf=", rf.getVotedFor(), "  args.LastLogIndex=", args.LastLogIndex, " ")
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
	Term                   int
	Success                bool
	FirstIndexConflictTerm int
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

	rf.logger(logger.DInfo, "Receive AppendEntries Request from N", args.LeaderId, " T", args.Term)
	// rf.logger(logger.DInfo, "Current entries: ", rf.getLog())

	reply.Term = rf.getCurrentTerm()
	reply.Success = false

	if args.Term < rf.getCurrentTerm() {
		rf.logger(logger.DError, "Refuse the AppendEntries Request args.Term < rf.getCurrentTerm()")
		return
	}

	rf.randomElectiontimeout()

	rf.mu.Lock()
	mLen := len(rf.log)
	// Find the first index of the current Term

	// for targetIndex > rf.commitIndex && rf.log[targetIndex-1].Term == rf.currentTerm {
	// 	targetIndex -= 1
	// }
	// reply.FirstIndexConflictTerm = targetIndex - 1
	targetIndex := mLen
	for targetIndex > 0 && rf.log[targetIndex-1].Term == args.Term {
		targetIndex -= 1
	}
	reply.FirstIndexConflictTerm = targetIndex + 1

	if mLen < args.PrevLogIndex || args.PrevLogIndex >= 1 && rf.log[args.PrevLogIndex-1].Term != args.PreLogTerm {
		rf.logger(logger.DError, "Doesn't contain an entry at prevLogIndex ", args.PrevLogIndex, " whose term matches prevLogTerm")
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	rf.votedFor = args.LeaderId
	// Find the matched
	rf.logger(logger.DLog, "New comming log entries start at index ", args.PrevLogIndex+1, ":", args.Entries)
	for index := 0; index < len(args.Entries); index += 1 {
		curLen := mLen
		if args.PrevLogIndex+index < curLen {
			if rf.log[args.PrevLogIndex+index].Term != args.Entries[index].Term {
				// conflic
				rf.logger(logger.DWarn, "Log Entry Conflict happens at index ", args.PrevLogIndex+index+1, ", remove the entries start from this place.")
				// rf.appendLogEntriesAt(args.PrevLogIndex+index, args.Entries[index:])
				rf.mu.Unlock()
				rf.removeFollowingLogEntriesWithLock(args.PrevLogIndex + index)
				return
			}
		} else {
			rf.logger(logger.DLog, "Append log entries: ", args.Entries[index:])
			rf.appendLogEntriesAt(args.PrevLogIndex+index, args.Entries[index:])
			break
		}
	}
	rf.mu.Unlock()
	if args.LeaderCommit > rf.getCommitIndex() {
		rf.logger(logger.DLog, "args.LeaderCommit > rf.getCommitIndex()")
		ci := len(rf.getLog())
		if args.LeaderCommit < ci {
			ci = args.LeaderCommit
		}
		rf.setCommitIndex(ci)
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
	// Your code here (2B).
	if rf.getRole() == LEADER {
		entries := make([]LogEntry, 1)
		entries[0] = LogEntry{rf.getCurrentTerm(), command}
		index, term = rf.appendLogEntriesWithLock(entries)
	}
	defer rf.logger(logger.DLog2, "index = ", index, " term = ", term, " isLeader = ", rf.getRole() == LEADER)
	return index, term, rf.getRole() == LEADER
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
	replyCount := 0
	var replyCountMutex sync.Mutex
	rf.setRole(CANDIDATE)
	rf.setCurrentTerm(rf.getCurrentTerm() + 1)
	rf.setVotedFor(rf.me)
	rf.randomElectiontimeout()
	voteChannel <- rf.getCurrentTerm()

	// voteChannel <- rf.currentTerm
	for i := 0; i < len(rf.peers) && !rf.killed(); i += 1 {
		go func(i int) {
			if i != rf.me {
				logs := rf.getLog()
				args := RequestVoteArgs{}
				args.Term = rf.getCurrentTerm()
				args.CandidateId = rf.me
				args.LastLogIndex = len(logs)
				if args.LastLogIndex == 0 {
					args.LastLogTerm = 0
				} else {
					args.LastLogTerm = logs[args.LastLogIndex-1].Term
				}
				reply := RequestVoteReply{}
				rf.logger(logger.DVote, "Send vote Requset to N", i)
				ok := rf.sendRequestVote(i, &args, &reply)
				if ok {
					rf.logger(logger.DVote, "Receive reply from N", i)
					if reply.VoteGranted {
						term := reply.Term
						voteChannel <- term
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

				replyCountMutex.Lock()
				replyCount += 1
				if replyCount >= len(rf.peers) {
					close(voteChannel)
				}
				replyCountMutex.Unlock()
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
			electionResult := rf.startElection()
			if electionResult {
				rf.setRole(LEADER)
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
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i += 1 {
					rf.setNextIndex(i, 1+len(rf.getLog()))
				}

				for !rf.killed() && rf.getRole() == LEADER {
					// check matchIndex, find the N, N > commitIndex && N <= majority of matchIndex && log[N].term == currentTerm
					matchIndexArray := make([]int, len(rf.getMatchIndexAll()))
					copy(matchIndexArray, rf.getMatchIndexAll())
					sort.Slice(matchIndexArray, func(i, j int) bool {
						return matchIndexArray[i] < matchIndexArray[j]
					})
					N := matchIndexArray[len(matchIndexArray)/2]
					if N > rf.getCommitIndex() && rf.getLog()[N-1].Term == rf.getCurrentTerm() {
						rf.logger(logger.DCommit, "Found an N > commitIndex && N <= majority of matchIndex && log[N].term == currentTerm")
						rf.setCommitIndex(N)
					}

					rf.setMatchIndex(rf.me, len(rf.getLog()))
					rf.setNextIndex(rf.me, len(rf.getLog())+1)
					rf.logger(logger.DLeader, "Current matchIndex: ", rf.getMatchIndexAll(), " commitIndex: ", rf.getCommitIndex(), " nextIndex: ", rf.getNextIndexAll(), " len(rf.log): ", len(rf.getLog()))
					// rf.logger(logger.DLeader, "Entries: ", rf.getLog())
					for i := 0; i < len(rf.peers); i += 1 {
						go func(i int, timeout int) {
							if i == rf.me {
								return
							}
							rf.logger(logger.DLeader, "rf.getNextIndex(i)=", rf.getNextIndex(i), " ,len(rf.getLog())=", len(rf.getLog()))
							t0 := time.Now()
							for time.Since(t0).Seconds() < 3 && !rf.killed() && rf.getRole() == LEADER && timeout == rf.getElectiontimeout() {
								args := AppendEntriesArgs{}
								args.Term = rf.getCurrentTerm()
								args.LeaderId = rf.me
								args.LeaderCommit = rf.getCommitIndex()
								args.PrevLogIndex = rf.getNextIndex(i) - 1
								rf.mu.Lock()
								if args.PrevLogIndex-1 >= 0 && args.PrevLogIndex-1 < len(rf.log) {
									args.PreLogTerm = rf.log[args.PrevLogIndex-1].Term
								} else {
									args.PreLogTerm = 0
								}
								args.Entries = rf.log[args.PrevLogIndex:]
								rf.mu.Unlock()

								rf.logger(logger.DLeader, "Send AppendEntries to N", i, " len(Entries)= ", len(args.Entries))
								reply := AppendEntriesReply{}
								ok := rf.sendAppendEntries(i, &args, &reply)
								rf.logger(logger.DLeader, "Send Entry at Index: ", args.PrevLogIndex, " to N", i, " len(entries)=", len(args.Entries))
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
										// Optimization: decrease the  nextIndex[i] back to the first index of conflict Term
										pre := rf.getNextIndex(i)
										if reply.FirstIndexConflictTerm >= pre {
											rf.logger(logger.DWarn, "NextIndex doesn't decrease! reply.FirstIndexConflictTerm: ", reply.FirstIndexConflictTerm, " >= pre: ", pre)
											if pre > 1 {
												rf.logger(logger.DWarn, "Find the index of the first entry with term = ", rf.getLog()[pre-2].Term)
												firstIndexConflictTerm := pre - 1
												for firstIndexConflictTerm > 0 && rf.getLog()[firstIndexConflictTerm-1].Term == rf.getLog()[pre-2].Term {
													firstIndexConflictTerm -= 1
												}
												rf.setNextIndex(i, firstIndexConflictTerm)
											}
										} else {
											rf.setNextIndex(i, reply.FirstIndexConflictTerm)
										}

										rf.logger(logger.DLog, "Fail to replicate log entries, decrease nextIndex[", i, "] ", pre, " -> ", rf.getNextIndex(i), " matchIndex[", i, "] = ", rf.getMatchIndex(i))
										if rf.getNextIndex(i) <= 0 {
											rf.logger(logger.DError, "nextIndex[", i, "] out of boundry, error condition! Stop replicating entries.")
											rf.setNextIndex(i, 1)
											break
										}
									}

								} else {
									rf.logger("Fail to receive reply from N", i)
								}
							}
						}(i, rf.getElectiontimeout())
					}
					time.Sleep(time.Duration(rf.heartbeatInterval) * time.Millisecond)
				}
			}
			time.Sleep(time.Duration(rf.getElectiontimeout()) * time.Millisecond)
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
	rf.lastApplyIndex = 0
	rf.logger(logger.DInfo, "Make a newborn")

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.syncApplyCh()
	return rf
}
