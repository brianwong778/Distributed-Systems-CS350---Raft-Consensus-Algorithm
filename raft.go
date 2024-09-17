package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"bytes"
	"cs350/labrpc"
	"encoding/gob"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()

	// Your data here (4A, 4B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	//Volatile state on all servers
	commitIndex int
	lastApplied int

	//Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	//keep track of the state
	state string //"Leader", "Candidate", "Follower"

	//channels
	receiveHeartbeat chan bool
	voteResults      chan bool
	applyCh          chan ApplyMsg
	resetTimerCh     chan bool
}

// each entry contains command for state machine, and term when entry was received by leader (first index is 1)
type LogEntry struct {
	Command interface{}
	Term    int
}

const HeartbeatInterval = 120 * time.Millisecond

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//var term int
	//var isleader bool
	// Your code here (4A).

	//commenting out for 4C

	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	isLeader := rf.state == "Leader"

	return rf.currentTerm, isLeader
}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4B).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	// encode currentTerm, votedFor, and log
	if error := e.Encode(rf.currentTerm); error != nil {
		log.Fatalf("Failed to encode currentTerm: %v", error)
	}
	if error := e.Encode(rf.votedFor); error != nil {
		log.Fatalf("Failed to encode votedFor: %v", error)
	}
	if error := e.Encode(rf.log); error != nil {
		log.Fatalf("Failed to encode log: %v", error)
	}

	//obtain byte slice containing the encoded data
	data := w.Bytes()

	//save the encoded state to perssist storage
	rf.persister.SaveRaftState(data)

	DPrintf("[persist] Server %d: Persisted state with currentTerm=%d, votedFor=%d, logSize=%d", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))

}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4B).
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
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("Failed to decode Raft's persisted state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}

}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (4A, 4B).
	Term         int //candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (4A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderId     int        // So follower can redirect clients
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int        // Leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // CurrentTerm, for leader to update itself
	Success bool // True if follower contained entry matching prevLogIndex and prevLogTerm

	//extra parameters, based on the student guide, for TestFailAgree4B
	ConflictIndex int // Index where the conflict starts
	ConflictTerm  int // Term of the conflicting entry
}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (4A, 4B).

	rf.mu.Lock()
	DPrintf("[RequestVote] Server %d: Received RequestVote from Candidate %d. Candidate's Term: %d, Server's Current Term: %d, Server's VotedFor: %d, Candidate's Last Log Index: %d, Candidate's Last Log Term: %d, Server's Last Log Index: %d, Server's Last Log Term: %d",
		rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor,
		args.LastLogIndex, args.LastLogTerm, len(rf.log)-1, rf.log[len(rf.log)-1].Term)

	defer rf.mu.Unlock()

	//Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		//reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//If the term in the RPC is newwer than the server's current term, update the term and revert to follower state
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "Follower"
		rf.votedFor = -1
		rf.persist()

		rf.resetTimer()

		DPrintf("[RequestVote] Server %d: Updated to new term %d and reverted to follower due to higher term from %d",
			rf.me, rf.currentTerm, args.CandidateId)

		rf.clearOutChannel(rf.receiveHeartbeat)
		rf.clearOutChannel(rf.voteResults)

	}

	// Initialize reply
	//reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// "If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)""
	// Check if this server hasn't voted for anyone or has voted for this candidateId, and the candidate's log is up-to-date
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.logIsUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
	}
	DPrintf("[RequestVote] Server %d: Responding to %d | VoteGranted: %v", rf.me, args.CandidateId, reply.VoteGranted)

}

/*

"Raft determines which of two logs is more up-to-date
by comparing the index and term of the last entries in the
logs. If the logs have last entries with different terms, then
the log with the later term is more up-to-date. If the logs
end with the same term, then whichever log is longer is
more up-to-date."

*/

func (rf *Raft) logIsUpToDate(candidateLastLogIndex, candidateLastLogTerm int) bool {
	// Get the last log index and term from the receiver's log
	lastLogIndex := rf.getLastIndex()
	lastLogTerm := rf.getLastTerm()

	DPrintf("[logIsUpToDate] Server %d: Checking log up-to-dateness | CandidateLastLogIndex: %d, CandidateLastLogTerm: %d, LastLogIndex: %d, LastLogTerm: %d", rf.me, candidateLastLogIndex, candidateLastLogTerm, lastLogIndex, lastLogTerm)

	// compare the terms of the last log entries
	if candidateLastLogTerm != lastLogTerm {
		result := candidateLastLogTerm > lastLogTerm
		DPrintf("[logIsUpToDate] Server %d: Log is up to date result (term comparison): %v (Candidate Term: %d, Server Term: %d)", rf.me, result, candidateLastLogTerm, lastLogTerm)
		return result
	}
	// If the terms are the same, compare the indices
	result := candidateLastLogIndex >= lastLogIndex
	DPrintf("[logIsUpToDate] Server %d: Log is up to date result (index comparison): %v (Candidate Last Index: %d, Server Last Index: %d)", rf.me, result, candidateLastLogIndex, lastLogIndex)
	return result
}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
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
// Look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("[sendRequestVote] Server %d: Sending RequestVote to %d | Term: %d", rf.me, server, args.Term)

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		DPrintf("Server killed")
		return
	}

	DPrintf("[AppendEntries] Follower %d: Received from Leader %d, Term %d, PrevLogIndex %d, Entries: %+v", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.Entries)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false // Default to false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	DPrintf("[AppendEntries] Server %d: Current Log: %+v", rf.me, rf.log)

	// Printing what leader is trying to append
	if len(args.Entries) > 0 {
		DPrintf("[AppendEntries] Server %d: Leader %d is trying to append entries: %+v", rf.me, args.LeaderId, args.Entries)
	} else {
		DPrintf("[AppendEntries] Server %d: Leader %d sent a heartbeat with no entries to append.", rf.me, args.LeaderId)
	}

	// 1. "Reply false if term < currentTerm (§5.1)" PART A
	if args.Term < rf.currentTerm {
		reply.Success = false
		DPrintf("[AppendEntries, bullet 1] Server %d: Rejecting AppendEntries RPC; Leader's Term: %d, Server's Current Term: %d, Reason: Leader's term is less than Server's current term.", rf.me, args.Term, rf.currentTerm)
		return

	}

	//If args.Term > rf.currentTerm, Leader demotes: update currentTerm, convert to follower, and reset votedFor
	if args.Term > rf.currentTerm {
		rf.state = "Follower"
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()

		rf.resetTimer()
		rf.clearOutChannel(rf.receiveHeartbeat)
		rf.clearOutChannel(rf.voteResults)
	}

	//send a heartbeat
	select {
	case rf.receiveHeartbeat <- true:
		DPrintf("[AppendEntries] Server %d received a heartbeat from Leader %d", rf.me, args.LeaderId)
	default:
	}

	// 2. "Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)" PART B

	//"If a follower does not have prevLogIndex in its log, it should return with conflictIndex = len(log) and conflictTerm = None."- Student guide
	if args.PrevLogIndex > rf.getLastIndex() {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1 // no term to match since the index is out of range
		DPrintf("[AppendEntries, bullet 2] Server %d: No log entry at prevLogIndex %d, log length %d. Sending conflictIndex %d, conflictTerm %d", rf.me, args.PrevLogIndex, len(rf.log), reply.ConflictIndex, reply.ConflictTerm)
		return
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {

		//"If a follower does have prevLogIndex in its log, but the term does not match,
		//it should return conflictTerm = log[prevLogIndex].Term, and then search its log for
		//the first index whose entry has term equal to conflictTerm." -student guide

		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		// Find the first index with ConflictTerm
		for index := args.PrevLogIndex; index >= 0; index-- {
			if rf.log[index].Term != reply.ConflictTerm {
				break
			}
			reply.ConflictIndex = index
		}
		DPrintf("[AppendEntries] Server %d: Log term mismatch at index %d, expected %d, found %d. Sending conflictIndex %d, conflictTerm %d", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log[args.PrevLogIndex].Term, reply.ConflictIndex, reply.ConflictTerm)
		return
	}

	// 3. "If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)" PART B
	index := args.PrevLogIndex + 1
	conflictDetected := false
	for i, entry := range args.Entries {
		if index+i < rf.getLastIndex() && rf.log[index+i].Term != entry.Term {
			rf.log = rf.log[:index+i] // Truncate the log
			DPrintf("[AppendEntries] Server %d: Truncating log under bullet 3, Current Log: %+v", rf.me, rf.log)
			conflictDetected = true
			break
		}
	}

	// 4. "Append any new entries not already in the log" PART B
	if index <= rf.getLastIndex() || conflictDetected {
		rf.log = append(rf.log[:index], args.Entries...)
		rf.persist()
		DPrintf("[AppendEntries, 4] Server %d: Replaced existing entries starting from index %d with new entries: %+v. New log: %+v", rf.me, index, args.Entries, rf.log)
	} else if len(args.Entries) > 0 {
		rf.log = append(rf.log, args.Entries...)
		rf.persist()
		DPrintf("[AppendEntries,4 ] Server %d: Extended log with new entries: %+v. New log: %+v", rf.me, args.Entries, rf.log)
	}

	reply.Success = true
	DPrintf("[AppendEntries] Server %d: replied true at the end, Updated Log: %+v", rf.me, rf.log)

	// 5. "If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)" PART B
	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := min(args.LeaderCommit, rf.getLastIndex())
		DPrintf("[AppendEntries, bullet 5] Server %d: old commit index: %d, new commit index: %d", rf.me, rf.commitIndex, newCommitIndex)
		rf.commitIndex = newCommitIndex
		DPrintf("[AppendEntries, bullet 5] Server %d: calling UpdateStateMachine. New log: %+v", rf.me, rf.log)
		go rf.UpdateStateMachine()
	}

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastTerm() int {
	// if len(rf.log) == 0 {
	// 	return 0 // Return a default term value when log is empty
	// }
	lastIndex := rf.getLastIndex()
	lastTerm := rf.log[lastIndex].Term
	return lastTerm

}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.killed() {
		DPrintf("server killed in Start()")
		return -1, -1, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//check if this is the leader
	if rf.state != "Leader" {
		return -1, rf.currentTerm, false
	}

	//index is the location o fthe last index where the command is being placed
	index := len(rf.log)
	term := rf.currentTerm
	logEntry := LogEntry{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, logEntry)
	rf.persist()

	if rf.state == "Leader" {
		DPrintf("[Start] Leader %d: Appended new log entry %+v at index %d", rf.me, logEntry, index)
		DPrintf("[Start] Leader %d: Current Log after appending new entry: %+v", rf.me, rf.log)
	}

	rf.persist()
	//rf.broadcastAppendEntries()//start broadcasting immediately
	//DPrintf("[Start] Leader %d: Received command | Command: %+v, Index: %d, Term: %d", rf.me, command, index, rf.currentTerm)

	//return lastIndex, rf.currentTerm, true
	return index, term, true
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetTimer() {
	select {
	case rf.resetTimerCh <- true:
	default:
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		DPrintf("[ticker] Server %d: at the top of ticker", rf.me)
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case "Leader":
			DPrintf("[ticker] Leader %d: Insdie leader case, haven't slept ", rf.me)
			select {
			case <-rf.resetTimerCh:
				DPrintf("[ticker] Leader %d: received signal from resetTimerCh ", rf.me)
				continue
			default:

				DPrintf("[ticker] Leader %d: Starting next sleep", rf.me)
				time.Sleep(HeartbeatInterval)
				DPrintf("[ticker] Leader %d: Sending heartbeats", rf.me)
				rf.broadcastAppendEntries()
			}

		case "Candidate", "Follower":
			electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond //random election timeout between 150-300ms
			DPrintf("[ticker] Server %d: Waiting for election timeout as %s", rf.me, state)
			select {
			case <-time.After(electionTimeout):
				DPrintf("[ticker] Server %d: Heartbeat timeout occured", rf.me)
				rf.startElection()
				rf.clearOutChannel(rf.receiveHeartbeat)
			case <-rf.receiveHeartbeat:
				DPrintf("received a heartbeat in Ticker()")
				rf.clearOutChannel(rf.receiveHeartbeat)
				DPrintf("[ticker] Server %d: Received a heartbeat, resetting timer.", rf.me)
				rf.clearOutChannel(rf.voteResults)
				// Received a heartbeat, nothing happens
			case <-rf.resetTimerCh:
				DPrintf("[ticker] Server %d: Reset timer signal received.", rf.me)
				rf.clearOutChannel(rf.resetTimerCh)

			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) clearOutChannel(ch chan bool) {
	for {
		select {
		case <-ch:
			// Read and discard
		default:
			// Channel is empty
			return
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	DPrintf("at the top of broadcastAppendEntries()")
	rf.mu.Lock()

	if rf.killed() {
		DPrintf("Leader is killed, stopping heartbeats")
		return
	}

	if rf.state != "Leader" {
		rf.mu.Unlock()
		return
	}
	//current term, leaderID, and commit index
	term := rf.currentTerm
	leaderID := rf.me
	leaderCommit := rf.commitIndex

	//use built in copy to avoid over complicating the locking within the go rountine
	nextIndexes := make([]int, len(rf.nextIndex))
	copy(nextIndexes, rf.nextIndex) // use a copy of next indexes to avoid needing to directly modify the original nextIndex

	rf.mu.Unlock()

	for server := range rf.peers {
		if server != rf.me {
			rf.mu.Lock()

			prevLogIndex := rf.nextIndex[server] - 1
			prevLogTerm := -1 //initalize to -1 to avoid trying to index out of bounds
			if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
				prevLogTerm = rf.log[prevLogIndex].Term
			}
			entries := rf.log[rf.nextIndex[server]:]

			DPrintf("[broadcastAppendEntries] Leader %d: Sending entries to server %d starting from index %d: %+v", rf.me, server, rf.nextIndex[server], entries)

			rf.mu.Unlock()

			DPrintf("[broadcastAppendEntries] Leader %d: Sending AppendEntries to %d with PrevLogIndex %d, Entries: %+v", rf.me, server, prevLogIndex, entries)
			go func(server int, term int, leaderID int, prevLogIndex int, prevLogTerm int, entries []LogEntry, leaderCommit int) {
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     leaderID,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: leaderCommit,
				}

				var reply AppendEntriesReply
				rf.sendAppendEntries(server, &args, &reply)

			}(server, term, leaderID, prevLogIndex, prevLogTerm, entries, leaderCommit)
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// success := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// return success

	DPrintf("Sending AppendEntries() to server %d", server)

	// Send the AppendEntries RPC to the follower server
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); !ok {
		DPrintf("[sendAppendEntries] Leader %d: AppendEntries RPC to server %d failed", rf.me, server)
		return false
	}
	DPrintf("[sendAppendEntries] Leader %d: Received response from server %d | Success: %v, Term: %d", rf.me, server, reply.Success, reply.Term)

	// The actual processing of the response (updating state, adjusting nextIndex/matchIndex, etc.) is now handled separately
	rf.handleAppendEntriesReply(server, args, reply)
	return true

}

func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// "If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)"
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = "Follower"
		rf.persist()
		DPrintf("[handleAppendEntriesReply] Server %d: Stepping down to Follower due to higher term received from server %d. Old Term: %d, New Term: %d, New State: Follower",
			rf.me, server, rf.currentTerm, reply.Term)
		rf.resetTimer()
		rf.clearOutChannel(rf.receiveHeartbeat)
		rf.clearOutChannel(rf.voteResults)
		return
	}

	// If the term of the reply is less than the currentTerm, ignore this response.
	if reply.Term < rf.currentTerm {
		DPrintf("[handleAppendEntriesReply] Leader %d: Ignored outdated response from %d with term %d", rf.me, server, reply.Term)
		return
	}

	// Upon success: "update nextIndex and matchIndex for follower (§5.3)"
	if rf.state == "Leader" && args.Term == rf.currentTerm {
		if reply.Success {
			oldNextIndex := rf.nextIndex[server]
			oldMatchIndex := rf.matchIndex[server]
			if rf.matchIndex[server] < args.PrevLogIndex+len(args.Entries) {
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				DPrintf("[handleAppendEntriesReply, Sucess] Leader %d: Condition met: rf.matchIndex[server] < args.PrevLogIndex + len(args.Entries)", rf.me)
			}

			DPrintf("[handleAppendEntriesReply, Sucess] Leader %d: Updated matchIndex[%d] from %d to %d", rf.me, server, oldMatchIndex, rf.matchIndex[server])
			DPrintf("[handleAppendEntriesReply, Sucess] Leader %d: Updated nextIndex[%d] from %d to %d", rf.me, server, oldNextIndex, rf.nextIndex[server])

		} else {
			//"decrement nextIndex and retry (§5.3)"

			//"Upon receiving a conflict response, the leader should first search its log for conflictTerm.
			//If it finds an entry in its log with that term, it should set nextIndex to be the one beyond
			//the index of the last entry in that term in its log.
			//If it does not find an entry with that term, it should set nextIndex = conflictIndex." -student guide

			if reply.ConflictTerm == -1 {
				DPrintf("[handleAppendEntriesReply, Failure] Leader %d: No specific conflict term provided, setting nextIndex to conflictIndex %d", rf.me, reply.ConflictIndex)
				rf.nextIndex[server] = reply.ConflictIndex
			} else {
				lastIndexOfTerm := -1
				for i := range rf.log {
					if rf.log[i].Term == reply.ConflictTerm {
						lastIndexOfTerm = i // Find the last index with the conflicting term
					}
				}
				if lastIndexOfTerm != -1 {
					rf.nextIndex[server] = lastIndexOfTerm + 1
					DPrintf("[handleAppendEntriesReply, Failure] Leader %d: Found last index of conflict term %d at %d, setting nextIndex to %d", rf.me, reply.ConflictTerm, lastIndexOfTerm, lastIndexOfTerm+1)
				} else {
					rf.nextIndex[server] = reply.ConflictIndex
					DPrintf("[handleAppendEntriesReply, Failure] Leader %d: Did not find conflict term %d, setting nextIndex to conflictIndex %d", rf.me, reply.ConflictTerm, reply.ConflictIndex)
				}
			}
		}
		rf.handleCommitIndex()
	}
}

func (rf *Raft) handleCommitIndex() {
	if rf.state != "Leader" {
		return
	}

	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	DPrintf("[handleCommitIndex] Leader %d: Evaluating possible commit index update", rf.me)

	//start N from the last log index and decrement towards the current commitIndex
	for N := rf.getLastIndex(); N >= rf.commitIndex; N-- {
		//count the number of servers with the same log entry at index N
		numCopies := 1 //count the leader

		// "If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N,
		// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)."
		for server := range rf.peers {
			if server != rf.me && rf.matchIndex[server] >= N && rf.log[N].Term == rf.currentTerm {
				numCopies++
			}
		}

		//check for a majority
		if numCopies > len(rf.peers)/2 {
			DPrintf("[handleCommitIndex] Leader %d: Updating commitIndex from %d to %d", rf.me, rf.commitIndex, N)
			rf.commitIndex = N
			go rf.UpdateStateMachine()
			break

		}
	}
}

func (rf *Raft) UpdateStateMachine() {
	if rf.killed() {
		DPrintf("UpdateStateMachine: Server has been stopped")
		return
	}

	rf.mu.Lock()
	DPrintf("[UpdateStateMachine] Server %d: Starting UpdateStateMachine with lastApplied=%d, commitIndex=%d", rf.me, rf.lastApplied, rf.commitIndex)

	// "All Servers: If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)"
	for entryIndex := rf.lastApplied + 1; entryIndex <= rf.commitIndex; entryIndex++ {
		committedEntry := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[entryIndex].Command,
			CommandIndex: entryIndex,
		}

		// select {
		// case rf.applyCh <- committedEntry:
		// 	DPrintf("[UpdateStateMachine] Server %d: Applied log entry to state machine. Index: %d, Command: %+v", rf.me, entryIndex, rf.log[entryIndex].Command)
		// default:
		// 	// avoid blocking
		// }

		rf.applyCh <- committedEntry
		DPrintf("[UpdateStateMachine] Server %d: Applied log entry to state machine. Index: %d, Command: %+v", rf.me, entryIndex, rf.log[entryIndex].Command)

		oldLastApplied := rf.lastApplied
		rf.lastApplied = entryIndex

		DPrintf("[UpdateStateMachine] Server %d: oldLastApplied: %d, new LastApplied: %d", rf.me, oldLastApplied, rf.lastApplied)

	}
	rf.mu.Unlock()
}

func (rf *Raft) startElection() {

	rf.mu.Lock()
	DPrintf("[startElection] Server %d: Starting election | Term: %d", rf.me, rf.currentTerm)

	if rf.state != "Candidate" {
		rf.state = "Candidate"
	}
	rf.currentTerm++
	rf.votedFor = rf.me
	currentTerm := rf.currentTerm
	lastLogIndex := rf.getLastIndex()
	lastLogTerm := rf.getLastTerm()
	rf.mu.Unlock()

	votesReceived := 1 //vote for self
	for server := range rf.peers {
		if server != rf.me {
			go func(server int) {
				args := RequestVoteArgs{
					Term:         currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				var reply RequestVoteReply

				if ok := rf.sendRequestVote(server, &args, &reply); ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm { //check if higher term found
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = "Follower"
						rf.resetTimer()
						return
					}

					// if reply.VoteGranted {
					// 	DPrintf("[startElection] Candidate %d: Received vote from %d | Total Votes: %d", rf.me, server, votesReceived+1)
					// } else {
					// 	DPrintf("[startElection] Candidate %d: Vote denied by %d", rf.me, server)
					// }

					select {
					case rf.voteResults <- reply.VoteGranted: // Vote result sent
					default:
						// avoid blocking
					}
				} else {
					DPrintf("[startElection] Candidate %d: Failed to receive vote from %d", rf.me, server)
					select {
					case rf.voteResults <- false: // False vote result sent
					default:
						// avoid blocking
					}
				}
			}(server)
		}
	}

	// Collect votes
	for i := 0; i < len(rf.peers)-1; i++ {
		voteGranted := <-rf.voteResults
		rf.mu.Lock()
		// Check if still a candidate and the term hasn't changed
		if rf.state != "Candidate" || rf.currentTerm != currentTerm {
			rf.mu.Unlock()
			return
		}
		if voteGranted {
			votesReceived++
			// Become leader
			if votesReceived > len(rf.peers)/2 {
				rf.state = "Leader"
				rf.nextIndex = make([]int, len(rf.peers))  //from "Volatile state on Leaders" in Figure 2
				rf.matchIndex = make([]int, len(rf.peers)) //from "Volatile state on Leaders" in Figure 2

				DPrintf("[startElection] Server %d: Election resolved | New State: %s", rf.me, rf.state)

				for i := range rf.peers {
					rf.nextIndex[i] = rf.getLastIndex() + 1 // "for each server...initalize to leader last log index + 1"
					rf.matchIndex[i] = 0                    // "for each server...index of highest log entry knwon to be replicated on the server, init 0"
					DPrintf("[startElection] New Leader %d: Initialized nextIndex[%d] = %d, matchIndex[%d] = %d", rf.me, i, rf.nextIndex[i], i, rf.matchIndex[i])
				}
				rf.persist()
				rf.clearOutChannel(rf.voteResults)
				rf.mu.Unlock()

				//rf.broadcastAppendEntries() // WANT TO START BROADCASTING NOW, BUT CAUSES DEADLOCK

				return
			}
		}
		rf.mu.Unlock()
	}
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (4A, 4B).

	//initalize state for leader election
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0

	//start as a follower
	rf.state = "Follower"

	// Channels
	rf.receiveHeartbeat = make(chan bool)
	rf.voteResults = make(chan bool)
	rf.applyCh = applyCh
	rf.resetTimerCh = make(chan bool)

	//Indexand log
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.log = append(rf.log, LogEntry{Term: 0})

	// initialize from state persisted before a crash.
	rf.readPersist(persister.ReadRaftState())

	DPrintf("starting go routine in Make()")
	// start ticker goroutine to start elections.
	go rf.ticker()

	return rf
}
