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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

type Log struct {
	Logentries []Entry
}

func MakeLog(term int, index int) Log {
	log := Log{
		Logentries: make([]Entry, 0),
	}
	log.put(Entry{Term: term, Index: index})
	return log
}

func (l *Log) getBaseIndexAndTerm() (int, int) {
	return l.Logentries[0].Index, l.Logentries[0].Term
}

func (l *Log) getBaseIndex() int {
	return l.Logentries[0].Index
}

func (l *Log) getSize() int {
	return len(l.Logentries)
}

func (l *Log) getEntry(index int) Entry {
	idx := index - l.Logentries[0].Index
	return l.Logentries[idx]
}

func (l *Log) getLastIndexAndTerm() (int, int) {
	lastEntry := l.Logentries[l.getSize()-1]
	return lastEntry.Index, lastEntry.Term
}

func (l *Log) put(newEntry Entry) {
	l.Logentries = append(l.Logentries, newEntry)
}

func (l *Log) append(posIndex int, entires []Entry) {
	pos := posIndex - l.Logentries[0].Index
	for i, entry := range entires {
		idx := pos + i
		if idx >= len(l.Logentries) {
			l.Logentries = append(l.Logentries, entry)
		} else {
			l.Logentries[idx] = entry
		}
	}
}

func (l *Log) trim(index int, term int) {

}

func (l *Log) slice(index int) []Entry {
	baseIndex := l.Logentries[0].Index
	idx := index - baseIndex
	if idx < 0 {
		idx = 0
	} else if idx > len(l.Logentries) {
		idx = len(l.Logentries)
	}
	return l.Logentries[idx:]

}

type RaftState = int

const (
	Leader    RaftState = 0
	Follower  RaftState = 1
	Candidate RaftState = 2
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
	log          Log
	state        RaftState
	votedFor     int
	currentTerm  int
	electionTime time.Time
	votes        int

	nextIndex  map[int]int
	matchIndex map[int]int

	commitIndex        int
	lastApplied        int
	newCommitReadyChan chan struct{}
	applyCh            chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	var term, votedFor int
	var log Log
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		//   error...
	} else {
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = log
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).

	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		DebugPf(dVote, "%d Server's Term is higher than %d Candidate in RequestVote", rf.me, args.CandidateId)
		return
	}

	if args.Term > rf.currentTerm {
		DebugPf(dLog, "%d Server's term %d is higher than request receiver %d", args.CandidateId, args.Term, rf.me)
		rf.newTermL(args.Term)
	}

	// in same ticker time
	lastLogIndex_, lastLogTerm_ := rf.log.getLastIndexAndTerm()
	update := args.LastLogTerm > lastLogTerm_ || (args.LastLogTerm == lastLogTerm_ && args.LastLogIndex >= lastLogIndex_)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && update {
		//
		DebugPf(dVote, "%d Server Vote for %d Server in Term %d", rf.me, args.CandidateId, rf.currentTerm)
		rf.setElectionTime()
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
	}

	reply.Term = rf.currentTerm
	DebugPf(dVote, "%d Server return GrantedVoteReply %+v to %d Server", rf.me, reply, args.CandidateId)
	return
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		DebugPf(dCommit, "%d Server's Term is higher than %d Leader in AppendEntries", rf.me, args.LeaderId)
		return
	}

	if args.Term > rf.currentTerm || rf.state != Follower {
		rf.newTermL(args.Term)
	}

	if rf.state != Follower {
		rf.newTermL(args.Term)
	}

	rf.setElectionTime()

	baseIndex_, _ := rf.log.getBaseIndexAndTerm()
	lastIndex_, _ := rf.log.getLastIndexAndTerm()

	// prev log is same
	// default that prevLogIndex > baseIndex
	if args.PrevLogIndex <= lastIndex_ && args.PrevLogTerm == rf.log.getEntry(args.PrevLogIndex).Term {
		reply.Success = true
		// we need to check the final match on
		logInsertIndex := args.PrevLogIndex + 1
		newEntriesIndex := logInsertIndex
		// Initialize
		if args.PrevLogIndex <= baseIndex_ {
			// DPrintf("prevLogIndex:%d baseIndex %d", args.PrevLogIndex, baseIndex_)
			logInsertIndex = baseIndex_ + 1
			newEntriesIndex = logInsertIndex
		}
		// DebugPf(dWarn,"logInsertIndex:%d, ")

		// find the mismatch
		loglastIndex_, _ := rf.log.getLastIndexAndTerm()
		var argsbaseIndex_, argslastIndex_ int

		if len(args.Entries) != 0 {

			argslastIndex_ = args.Entries[len(args.Entries)-1].Index
			argsbaseIndex_ = args.Entries[0].Index

			for {
				if logInsertIndex > loglastIndex_ || newEntriesIndex > argslastIndex_ {
					break
				}

				if rf.log.getEntry(logInsertIndex).Term != args.Entries[newEntriesIndex-argsbaseIndex_].Term {
					break
				}

				logInsertIndex++
				newEntriesIndex++
				// DPrintf("LogInsert: %")
			}

			// some duplicate logs are useful and add these entries into log
			if newEntriesIndex <= argslastIndex_ {
				duplicateEntries := args.Entries[newEntriesIndex-argsbaseIndex_:]
				DebugPf(dInfo, "%d Server get Log from %d Server, LogInsertIndex: %d LogInfo %d", rf.me, args.LeaderId, logInsertIndex, duplicateEntries)
				rf.log.append(logInsertIndex, duplicateEntries)
				rf.persist()
				DebugPf(dInfo, "%d Server Log %+v", rf.me, rf.log)
			}

		}

		// update commitIndex
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = func(x int, y int) int {
				if x > y {
					return y
				} else {
					return x
				}
			}(args.LeaderCommit, rf.log.Logentries[rf.log.getSize()-1].Index)
			// process commited entries
			// commitIndex update and invoke processNewCommitLog
			rf.newCommitReadyChan <- struct{}{}
			DebugPf(dCommit, "%d Follower change CommitIndex from %d to %d", rf.me, rf.lastApplied, rf.commitIndex)
		}
	}

	reply.Term = rf.currentTerm
	DebugPf(dCommit, "%d Server return to %d Server AppendEntries %+v", rf.me, args.LeaderId, reply)

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.state == Leader
	if !isLeader {
		return index, term, isLeader
	}
	baseIndex, _ := rf.log.getBaseIndexAndTerm()
	index = baseIndex + rf.log.getSize()
	term = rf.currentTerm
	rf.log.put(Entry{Command: command, Index: index, Term: term})
	DPrintf("Applier input Command : %+v", command)
	rf.persist()
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.tickL()
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) tickL() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ms := 150
	ms = ms + rand.Int()%150
	timeOutDurantion := time.Duration(ms) * time.Millisecond
	if rf.state == Leader {
		// Leader Rule
		rf.setElectionTime()
		rf.startLeaderL(true)
	}
	if t := time.Since(rf.electionTime); t >= timeOutDurantion {
		// Candidate Rule
		rf.setElectionTime()
		rf.startElectionL()
	}
}

func (rf *Raft) setElectionTime() {
	rf.electionTime = time.Now()
	// ms := 450
	// ms = ms + rand.Int()%50
	// du := time.Duration(ms) * time.Millisecond
	// rf.electionTime = t.Add(du)
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
	// initialize from state persisted before a crash
	rf.currentTerm = 1
	rf.state = Follower
	rf.log = MakeLog(0, 0)
	rf.votedFor = -1
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	rf.applyCh = applyCh
	rf.lastApplied = 0
	rf.commitIndex = 0
	for idx, _ := range rf.peers {
		rf.nextIndex[idx] = 1
		rf.matchIndex[idx] = 0
	}
	rf.newCommitReadyChan = make(chan struct{}, 16)
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	rf.setElectionTime()
	go rf.ticker()
	go rf.processNewCommitLog()

	return rf
}

func (rf *Raft) startElectionL() {
	rf.currentTerm += 1
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.persist()
	lastLogIndex_, lastLogTerm_ := rf.log.getLastIndexAndTerm()
	DebugPf(dTerm, "%d Server start election in Term %d", rf.me, rf.currentTerm)
	// Args
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex_,
		LastLogTerm:  lastLogTerm_,
	}
	// shared data
	rf.votes = 1
	for idx, _ := range rf.peers {
		if idx != rf.me {
			DebugPf(dVote, "%d Server Send RequestVote to Server %d, ArgsInfo : %+v", rf.me, idx, args)
			go rf.requestAndProcessVote(idx, args)
		}
	}
}

func (rf *Raft) requestAndProcessVote(peerId int, args RequestVoteArgs) {
	reply := RequestVoteReply{
		Term:        args.Term,
		VoteGranted: false,
	}
	ok := rf.sendRequestVote(peerId, &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// check the Term
		if reply.Term > rf.currentTerm {
			// become Follower
			DebugPf(dTerm, "%d Server from Term %d jump to Term %d, Since the reject from %d Server", rf.me, rf.currentTerm, reply.Term, peerId)
			rf.newTermL(reply.Term)
			return
		}

		if args.Term != rf.currentTerm || rf.state == Leader {
			// situation1 : new Ticker start, old operation no use
			// server has got majority vote, no need to get others vote
			DebugPf(dVote, "%d Server has become a leader or jumped to another term", rf.me)
			return
		}

		if reply.Term == rf.currentTerm {
			// process the reply
			if reply.VoteGranted {
				rf.votes += 1
				if rf.votes*2 > len(rf.peers) {
					rf.setLeaderL()
					// directly send a heartbeats
				}
			}
		}

	}
}

func (rf *Raft) setLeaderL() {
	rf.state = Leader
	rf.startLeaderL(true)
	DebugPf(dLeader, "%d Server win majority votes and become a Leader in Term %d", rf.me, rf.currentTerm)
	//initialize nextIndex and MatchIndex which are in volatie state
	lastIndex, _ := rf.log.getLastIndexAndTerm()
	for k, _ := range rf.nextIndex {
		rf.nextIndex[k] = lastIndex + 1
		rf.matchIndex[k] = 0
	}
}

func (rf *Raft) startLeaderL(heartbeat bool) {
	for idx, _ := range rf.peers {
		if idx != rf.me {

			nextIndex_ := rf.nextIndex[idx]
			// nextIndex is a guess
			lastIndex_, _ := rf.log.getLastIndexAndTerm()
			// baseIndex_, _ := rf.log.getBaseIndexAndTerm()

			if lastIndex_ >= nextIndex_ || (heartbeat && lastIndex_+1 == nextIndex_) {

				prevIndex_ := nextIndex_ - 1
				prevTerm_ := rf.log.getEntry(prevIndex_).Term

				duplicateEntries := rf.log.slice(nextIndex_)

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevIndex_,
					PrevLogTerm:  prevTerm_,
					Entries:      duplicateEntries,
					LeaderCommit: rf.commitIndex,
				}
				go rf.sendAndProcessAppendEntries(idx, args)
				continue
			}

		}
	}
}

func (rf *Raft) sendAndProcessAppendEntries(peerId int, args AppendEntriesArgs) {
	var reply AppendEntriesReply
	DebugPf(dCommit, "%d Server send to %d Server  AppendEntriesArgs %+v", rf.me, peerId, args)
	ok := rf.sendAppendEntries(peerId, &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.newTermL(reply.Term)
			return
		}

		if args.Term != rf.currentTerm || rf.state != Leader {
			return
		}

		// args.term == rf.currenTerm == reply.term
		if reply.Success {
			// update nextIndex and matchIndex
			rf.nextIndex[peerId] += len(args.Entries)
			// very important , -1 , because the default previous one log
			rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
			rf.advanceCommitIndexL() // 5.4 update commitIndex
		} else {
			// conflict
			rf.nextIndex[peerId] -= 1
			baseIndex, _ := rf.log.getBaseIndexAndTerm()
			if rf.nextIndex[peerId] <= baseIndex {
				//TODO
				// SendSnapshot To deal with it
			}
		}
		// else {
		// 	// Heartbeat
		// }
	}
}

func (rf *Raft) advanceCommitIndexL() {

	for index := rf.commitIndex + 1; index < rf.log.getSize(); index++ {
		if rf.log.getEntry(index).Term != rf.currentTerm {
			continue
		}

		majority_count := 1
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= index && i != rf.me {
				majority_count += 1
			}
		}

		if majority_count*2 > len(rf.peers) {
			rf.commitIndex = index
		}
	}
	if rf.lastApplied != rf.commitIndex {
		// Process Commit Entries
		// commitIndex update and invoke processNewCommitLog
		rf.newCommitReadyChan <- struct{}{}
		DebugPf(dCommit, "%d Leader commitIndex update from %d to %d", rf.me, rf.lastApplied, rf.commitIndex)
	}
}

func (rf *Raft) newTermL(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	rf.state = Follower
	rf.setElectionTime()
}

// its an independent rountine
// if new log is commit , it will be invoked and put commit log into ApplyCh
func (rf *Raft) processNewCommitLog() {
	for range rf.newCommitReadyChan {
		rf.mu.Lock()
		var entries []Entry

		if rf.commitIndex > rf.lastApplied {
			DebugPf(dWarn, "CommitIndex %d, LastApplierIndex %d", rf.commitIndex, rf.lastApplied)
			entries = rf.log.Logentries[rf.lastApplied+1 : rf.commitIndex+1]
			rf.lastApplied = rf.commitIndex
			DebugPf(dWarn, "%d Server update entries %+v", rf.me, entries)
		}
		rf.mu.Unlock()

		// put the entires into applyCh
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid:  true,
				CommandIndex:  entry.Index,
				Command:       entry.Command,
				SnapshotValid: false,
			}
		}
	}

}
