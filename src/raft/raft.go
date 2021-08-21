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
	"os"
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

type LogEntry struct {
	Term    int
	Command interface{}
}

type RaftType int

const (
	Follower RaftType = iota
	Candidate
	Leader
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

	// 2A
	// id                 int
	state              RaftType
	currentTerm        int
	voteFor            int
	log                []LogEntry
	electionResetEvent time.Time

	// 2B
	nextIndex          map[int]int // map peerId to nextIndex
	commitIndex        int
	lastcommitIndex    int         // a helper variable which record the last value of commitIndex
	matchIndex         map[int]int // gather every peerId's state of duplication
	newCommitReadyChan chan struct{}
	applyCh            chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
// WARNING : we need lock mu before invoke GetState
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = rf.state == Leader
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

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogIndex, lastLogTerm := rf.getLastIndexAndTerm()
	DPrintf("RequestVote: %+v [currentTerm=%d, voteFor=%d]", args, rf.currentTerm, rf.voteFor)

	if args.Term > rf.currentTerm {
		DPrintf("... term out of date in RequestVote")
		rf.becomeFollower(args.Term)
	}

	// vote

	if rf.currentTerm == args.Term &&
		(rf.voteFor == -1 || rf.voteFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
	DPrintf("... Request reply: %+v", reply)

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("AppendEntries: %+v", args)

	if args.Term > rf.currentTerm {
		DPrintf("... term out of date in AppendEntries")
		rf.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == rf.currentTerm {
		if rf.state != Follower {
			rf.becomeFollower(args.Term)
		}
		rf.electionResetEvent = time.Now()
		// if term is same , it means that beatheart is saved || reset the Ticker()
		// but whethear the entry is copied is not sured

		// Find an insertion point -- mismatch between Leader and follower
		// Follower's log should be consistant with Leader's
		// we need from PrevLog
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(rf.log) && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term) {
			reply.Success = true

			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			// find where is mismatched

			for {
				if logInsertIndex >= len(rf.log) || newEntriesIndex >= len(args.Entries) {
					break
				}

				if rf.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}

				logInsertIndex++
				newEntriesIndex++
			}

			if newEntriesIndex < len(args.Entries) {
				// append these Entries into local Log
				DPrintf("... inserting Entries from index %d in Follower %d", logInsertIndex, rf.me)
				rf.log = append(rf.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
			}

			// update CommitIndex in Follower

			if args.LeaderCommitIndex > rf.commitIndex {
				rf.commitIndex = func(a int, b int) int {
					if a > b {
						return b
					} else {
						return a
					}
				}(args.LeaderCommitIndex, len(rf.log)-1)
				DPrintf("... Set Follower Server %d commit Index %d", rf.me, rf.commitIndex)
				rf.newCommitReadyChan <- struct{}{}
			}
		}

		// reply is false , it means that in Leader Server it should change the PrevLogIndex in that Follower
	}
	reply.Term = rf.currentTerm
	DPrintf("AppendEntries reply : %+v", *reply)
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
	// index := -1
	// term := -1
	// isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%d Server Submit %v", rf.me, command)

	if rf.state == Leader {
		rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
		return len(rf.log) - 1, rf.currentTerm, true
	}

	return -1, -1, false
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

//generate an random timeout duration
func (rf *Raft) RaftElectionTimeout() time.Duration {
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	timeoutDuration := rf.RaftElectionTimeout()
	rf.mu.Lock()
	startTerm := rf.currentTerm
	rf.mu.Unlock()

	DPrintf("%d Server start an election Ticker  at term = %d", rf.me, startTerm)

	t := time.NewTicker(10 * time.Millisecond)
	// equal to sleep()
	defer t.Stop()

	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		<-t.C

		rf.mu.Lock()
		// it become a Leader, no need to use ticker
		if rf.state == Leader {
			DPrintf("Server %d become the Leader", rf.me)
			rf.mu.Unlock()
			return
		}

		if startTerm != rf.currentTerm {
			DPrintf("%d sever in election timer term changed from %d to %d, bailing out", rf.me, startTerm, rf.currentTerm)
			rf.mu.Unlock()
			return
		}

		//check timeduration
		if t := time.Since(rf.electionResetEvent); t >= timeoutDuration {
			rf.startElection()
			// In startElection  no need to lock the mutex
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}

}

// start election
// before call startElection , the source is been locked (check method ticker)
func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm += 1
	savedCurrentTerm := rf.currentTerm
	rf.electionResetEvent = time.Now()
	rf.voteFor = rf.me
	DPrintf("%d Server  becomes Candidate (currentTerm=%d); log=%v", rf.me, rf.currentTerm, rf.log)

	votesReceived := 1

	for idx, _ := range rf.peers {

		if idx == rf.me {
			// idx == me
			continue
		}

		go func(peerId int) {

			rf.mu.Lock()
			// defer rf.mu.Unlock()
			savedLastLogIndex, savedLastLogTerm := rf.getLastIndexAndTerm()
			rf.mu.Unlock()
			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  rf.me,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}
			var reply RequestVoteReply
			DPrintf("send RequestVote from %d to %d", rf.me, peerId)

			if ok := rf.sendRequestVote(peerId, &args, &reply); ok {
				rf.mu.Lock() // use state and term
				defer rf.mu.Unlock()
				DPrintf("%d server received RequestVoteReply %+v", rf.me, reply)

				if rf.state != Candidate {
					// win n + 1 vote (all vote is 2n + 1) become a Leader
					// or get other RPC msg and become a Follower
					// stop the whole process of election, Leader exist
					return
				}

				if reply.Term > savedCurrentTerm {
					DPrintf("term out of date in RequestVoteReply")
					rf.becomeFollower(reply.Term)
					return

				} else if reply.Term == savedCurrentTerm { //
					if reply.VoteGranted {
						//true
						votesReceived += 1
						// 这里使用了一个函数闭包的性质
						// 内层函数使用外层函数的变量，即使外层函数已经执行完毕
						// votesReceived 变量逃逸到了heap上
						if votesReceived*2 > len(rf.peers) {
							DPrintf("%d wins election with %d votes", rf.me, votesReceived)
							rf.state = Leader
							go rf.startLeader()
							return
						}

					}
				}
			}

		}(idx)
	}

	go rf.ticker()
}

func (rf *Raft) startLeader() {
	// cm.state = Leader

	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()

	for {
		rf.leaderSendHeartbeats()
		<-t.C
		// DPrintf("%d StartLeader Label", rf.me)
		rf.mu.Lock()
		if rf.state != Leader {
			DPrintf("Server %d no leader anymore", rf.me)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}

}

func (rf *Raft) becomeFollower(term int) {
	DPrintf("%d becomes Follower with term=%d; log=%v", rf.me, term, rf.log)
	rf.state = Follower
	rf.currentTerm = term
	rf.voteFor = -1
	rf.electionResetEvent = time.Now()

	go rf.ticker()
}

// func (rf *Raft) sendHeartbeats() {
// 	rf.mu.Lock()
// 	savedCurrentTerm := rf.currentTerm
// 	rf.mu.Unlock()

// 	for idx, _ := range rf.peers {
// 		args := AppendEntriesArgs{
// 			Term:     savedCurrentTerm,
// 			LeaderId: rf.me,
// 		}
// 		if idx == rf.me {
// 			continue
// 		}
// 		go func(peerId int) {
// 			DPrintf("%d sending AppendEntries to %v: ni=%d, args=%+v", rf.me, peerId, 0, args)
// 			var reply AppendEntriesReply
// 			if ok := rf.sendAppendEntries(peerId, &args, &reply); ok {
// 				if reply.Term > savedCurrentTerm {
// 					// discovers server with higher term
// 					// become a follower
// 					rf.mu.Lock()
// 					defer rf.mu.Unlock()
// 					DPrintf("term ojut of date in heartbeat reply, server %d become follower", rf.me)
// 					rf.becomeFollower(reply.Term)
// 					return
// 				}
// 			}
// 		}(idx)
// 	}
// }

// 2B agreeMent protocal
func (rf *Raft) Submit(command interface{}) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%d Server Submit %v", rf.me, command)

	if rf.state == Leader {
		rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
		return true
	}
	return false
}

func (rf *Raft) leaderSendHeartbeats() {
	rf.mu.Lock()
	savedCurrentTerm := rf.currentTerm
	rf.mu.Unlock()

	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(peerId int) {
			rf.mu.Lock()
			nextLogIndex := rf.nextIndex[peerId]
			// prevLogIndex := nextLogIndex - 1
			// prevLogTerm := -1
			// if prevLogIndex >= 0 {
			// 	prevLogTerm = rf.log[prevLogIndex].Term
			// }
			prevLogIndex, prevLogTerm := rf.getPeerLastIndexAndTerm(peerId)
			duplicateEntries := rf.log[nextLogIndex:]

			args := AppendEntriesArgs{
				Term:              savedCurrentTerm,
				LeaderId:          rf.me,
				PrevLogIndex:      prevLogIndex,
				PrevLogTerm:       prevLogTerm,
				Entries:           duplicateEntries,
				LeaderCommitIndex: rf.commitIndex,
			}

			rf.mu.Unlock()

			var reply AppendEntriesReply
			if ok := rf.sendAppendEntries(peerId, &args, &reply); ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > savedCurrentTerm {
					rf.becomeFollower(reply.Term)
					return
				}
				// gather the commitIndex to gaurante commitIndex is matched
				if rf.state == Leader && reply.Term == savedCurrentTerm {
					if reply.Success {
						// success to duplicate the log in peerId server
						rf.nextIndex[peerId] = nextLogIndex + len(duplicateEntries)
						rf.matchIndex[peerId] = prevLogIndex + len(duplicateEntries)
						savedCommitIndex := rf.commitIndex
						//
						for i := savedCommitIndex + 1; i < len(rf.log); i++ {
							matchCount := 1
							if rf.log[i].Term == savedCurrentTerm {
								for idx, _ := range rf.peers {
									if idx == rf.me {
										continue
									}
									if rf.matchIndex[idx] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(rf.peers) {
									rf.commitIndex = i
								}
							}
						}

						// if commitedIndex update, invoke CommitReadyChan that Commited Entries Updated
						if rf.commitIndex != savedCommitIndex {
							DPrintf("%d server sets commitIndex := %d", rf.me, rf.commitIndex)
							rf.newCommitReadyChan <- struct{}{}
						}
					} else {
						rf.nextIndex[peerId] = nextLogIndex - 1
						DPrintf("Server %d AppendEntries reply from %d failed : nextIndex := %d", rf.me, peerId, nextLogIndex-1)
					}
				}
			}
		}(idx)
	}
}

// TODO:
func (rf *Raft) commitChanSender() {
	for range rf.newCommitReadyChan {
		// if some new Commits are updated, this code is invoked
		// otherwise the coroutine will be blocked
		rf.mu.Lock()
		// savedTerm := rf.currentTerm
		savedLastCommitIndex := rf.lastcommitIndex

		var entries []LogEntry
		if rf.commitIndex > rf.lastcommitIndex {
			//Actually it is always true
			entries = rf.log[rf.lastcommitIndex+1 : rf.commitIndex+1]
			rf.lastcommitIndex = rf.commitIndex
		}
		rf.mu.Unlock()

		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				Command:      entry.Command,
				CommandIndex: savedLastCommitIndex + i + 1,
				CommandValid: true,
			}
		}
		// commit thes entries
	}
}

// two helper function to Get PrevLogIndex (the initialized of it is -1)
func (rf *Raft) getLastIndexAndTerm() (int, int) {
	// if len(rf.log) != 0 {
	// 	lastIndex := len(rf.log) - 1
	// 	return lastIndex, rf.log[lastIndex].Term
	// } else {
	// 	return -1, -1
	// }
	lastIndex := len(rf.log) - 1
	return lastIndex, rf.log[lastIndex].Term
}

func (rf *Raft) getPeerLastIndexAndTerm(peerId int) (int, int) {
	// if rf.nextIndex[peerId] > 0 {
	// lastlogIndex := rf.nextIndex[peerId] - 1
	// return lastlogIndex, rf.log[lastlogIndex].Term
	// } else {
	// 	// no log and no last logTerm
	// 	return -1, -1
	// }
	lastlogIndex := rf.nextIndex[peerId] - 1
	return lastlogIndex, rf.log[lastlogIndex].Term
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
	rf.state = Follower
	rf.voteFor = -1
	// rf.lastcommitIndex = -1
	// rf.commitIndex = -1
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	rf.applyCh = applyCh

	for peerId, _ := range rf.peers {
		rf.nextIndex[peerId] = 1
		rf.matchIndex[peerId] = 0
	}

	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{
		0, nil,
	}
	rf.lastcommitIndex = 0
	rf.commitIndex = 0

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.newCommitReadyChan = make(chan struct{}, 16)
	rf.electionResetEvent = time.Now()
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.commitChanSender()

	return rf
}
