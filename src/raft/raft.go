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
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

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
	applyCh     chan ApplyMsg
	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	state         int
	timeOutIndex  int
	timeOutResult bool
	cond          *sync.Cond
}

//
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

//
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	Length  int
}

//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == 2 {
		isleader = true
	}
	rf.mu.Unlock()
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
	rf.mu.Lock()
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	rf.mu.Unlock()
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		//   error...
	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = log
		rf.mu.Unlock()
	}
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
	isLeader := false

	rf.mu.Lock()
	term = rf.currentTerm
	index = len(rf.log)

	if rf.state == 2 {
		isLeader = true
		DPrintf("%d: Start...%v", rf.me, command)
		rf.log = append(rf.log, LogEntry{Term: term, Index: index, Command: command})
	}

	rf.mu.Unlock()
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
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{})
	rf.commitIndex = 0
	rf.lastApplied = 0
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	rf.state = 0
	rf.timeOutIndex = 0
	rf.timeOutResult = false
	rf.cond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.raftStateHandler()
	return rf
}

//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	incomingIndex := rf.timeOutIndex
	DPrintf("%d: Receiving Vote request %v\n", rf.me, args)

	if args.Term < rf.currentTerm {
		DPrintf("%d: Denying Vote request %v due to higher term number %d\n", rf.me, args, rf.currentTerm)
		rf.mu.Unlock()
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = 0
		rf.votedFor = -1
	}

	followerLastTerm := rf.log[len(rf.log)-1].Term
	followerLastIndex := rf.log[len(rf.log)-1].Index
	if followerLastTerm > args.LastLogTerm || (followerLastTerm == args.LastLogTerm && followerLastIndex > args.LastLogIndex) {
		reply.VoteGranted = false
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
		}
		rf.mu.Unlock()
		rf.persist()
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID || rf.votedFor == rf.me {
		DPrintf("%d: Accepting Vote request %v\n", rf.me, args)
		rf.timeOutResult = false
		rf.votedFor = args.CandidateID
		outgoingIndex := rf.timeOutIndex
		rf.mu.Unlock()
		rf.persist()
		if incomingIndex == outgoingIndex {
			rf.cond.Signal()
		}
		reply.VoteGranted = true
		return
	}

	DPrintf("%d: Already voted for %d\n", rf.me, rf.votedFor)
	rf.mu.Unlock()
	reply.VoteGranted = false
	return

}

//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("%d: Receiving Heart beat %v\n", rf.me, args)
	currentTerm := rf.currentTerm

	if args.Term < currentTerm {
		DPrintf("%d: Denying heart beat %v\n", rf.me, args)
		reply.Success = false
		reply.Term = currentTerm
		rf.mu.Unlock()
		return
	}

	// There has to be a point till which the followers log matches the leader's log
	if args.PrevLogIndex >= len(rf.log) {
		DPrintf("%d: Followers log far behind\n", rf.me)
		DPrintf("%d: See the log during length conflict %v\n", rf.me, rf.log)
		reply.Success = false
		reply.Length = len(rf.log)
		rf.currentTerm = args.Term
		rf.state = 0
		rf.timeOutResult = false
		rf.mu.Unlock()
		rf.persist()
		rf.cond.Signal()
		reply.Term = currentTerm
		return
	}

	if rf.log[args.PrevLogIndex].Index != args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("%d: Follower requires log backup\n", rf.me)
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		reply.XIndex = rf.log[args.PrevLogIndex].Index
		DPrintf("%d: See the log during value %v\n", rf.me, rf.log)
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.log[i].Term != reply.XTerm {
				break
			}
			reply.XIndex--
		}
		reply.Success = false
		rf.currentTerm = args.Term
		rf.state = 0
		rf.timeOutResult = false
		rf.mu.Unlock()
		rf.persist()
		rf.cond.Signal()
		reply.Term = currentTerm
		return
	}

	// // if the leader is not sending any entry, you don't have to check any conflict
	// // and hencne also not allowed to do any kind of deletions
	// if len(args.Entries) == 0 {
	// 	reply.Success = true
	// 	temp := Min(args.LeaderCommit, args.PrevLogIndex)
	// 	rf.commitIndex = Max(rf.commitIndex, temp)
	// 	DPrintf("%d: Updating Follower Commit Index to %d\n", rf.me, rf.commitIndex)
	// 	goto updateCommitIndex
	// }

	// // If there's no entry to conflict then move to commit index handle
	// if len(rf.log) == args.PrevLogIndex+1 {
	// 	rf.log = append(rf.log, args.Entries...)
	// 	DPrintf("%d: Updated log %v\n", rf.me, rf.log)
	// 	reply.Success = true
	// 	temp := Min(args.LeaderCommit, args.PrevLogIndex+1)
	// 	rf.commitIndex = Max(rf.commitIndex, temp)
	// 	DPrintf("%d: Updating Follower Commit Index to %d\n", rf.me, rf.commitIndex)
	// 	goto updateCommitIndex
	// }

	// // If conflicting
	// if rf.log[args.PrevLogIndex+1].Term != args.Entries[0].Term {
	// 	DPrintf("%d: Removing Conflicting entries from %d\n", rf.me, args.PrevLogIndex+1)
	// 	rf.log = rf.log[0 : args.PrevLogIndex+1]
	// 	rf.log = append(rf.log, args.Entries...)
	// 	reply.Success = true
	// 	temp := Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	// 	rf.commitIndex = Max(rf.commitIndex, temp)
	// 	DPrintf("%d: Updating Follower Commit Index to %d\n", rf.me, rf.commitIndex)
	// 	goto updateCommitIndex
	// }

	// for i := 0; i < len(args.Entries); i++ {
	// 	if len(rf.log) < args.PrevLogIndex+1+i+1 {
	// 		rf.log = append(rf.log, args.Entries[i])
	// 	} else {
	// 		rf.log[args.PrevLogIndex+1+i] = args.Entries[i]
	// 	}
	// 	temp := Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	// 	rf.commitIndex = Max(rf.commitIndex, temp)
	// 	DPrintf("%d: Updating Follower Commit Index to %d\n", rf.me, rf.commitIndex)
	// }

	for i := 0; i < len(args.Entries); i++ {
		if len(rf.log) == args.PrevLogIndex+1+i {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}

		if rf.log[args.PrevLogIndex+1+i].Term != args.Entries[i].Term {
			rf.log = rf.log[:args.PrevLogIndex+1+i]
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	DPrintf("%d: Updating Follower Commit Index to %d\n", rf.me, rf.commitIndex)

	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			DPrintf("%d: Applying to the channel %v\n", rf.me, rf.log[i])
			rf.applyCh <- ApplyMsg{Command: rf.log[i].Command, CommandValid: true, CommandIndex: rf.log[i].Index}
		}
		rf.lastApplied = rf.commitIndex
	}
	reply.Success = true
	rf.currentTerm = args.Term
	rf.state = 0
	rf.timeOutResult = false
	rf.mu.Unlock()
	rf.persist()
	rf.cond.Signal()
	reply.Term = currentTerm
	return
}

//
func (rf *Raft) raftStateHandler() {

beginTimer:
	if rf.killed() {
		return
	}

	// Increase the timeOutIndex and start a goroutine that will timeout.
	rf.mu.Lock()

	DPrintf("%d: Next Index Reset to %d\n", rf.me, rf.timeOutIndex)

	rf.timeOutIndex++
	rf.timeOutResult = false
	go rf.timeOutHandler()
	rf.cond.Wait()
	timeOutResult := rf.timeOutResult
	if timeOutResult {
		DPrintf("%d: Timer Out\n", rf.me)
		if rf.state == 0 || rf.state == 1 {
			// We will need to start the elections
			rf.state = 1
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.mu.Unlock()
			rf.persist()
			go rf.startElections()
			goto beginTimer

		} else {
			// send the heartbeats
			rf.mu.Unlock()
			go rf.sendHeartBeats()
			goto beginTimer
		}
	} else {
		DPrintf("%d: Timer Reset\n", rf.me)
		rf.mu.Unlock()
		goto beginTimer
	}

}

//
func (rf *Raft) timeOutHandler() {

	rf.mu.Lock()
	state := rf.state
	incomingIndex := rf.timeOutIndex
	rf.mu.Unlock()

	duration := 0

	if state == 0 || state == 1 {
		duration = 400 + rand.Intn(400)
	} else {
		duration = 200
	}

	//
	// Wait for the time out
	time.Sleep(time.Duration(duration) * time.Millisecond)

	//
	// It is possible that the timer might have been restarted by someone
	// In which case, we won't send the signal to the calling function

	rf.mu.Lock()
	outgoingIndex := rf.timeOutIndex

	if incomingIndex == outgoingIndex {
		rf.timeOutResult = true
		rf.mu.Unlock()
		rf.cond.Signal()
		return
	}
	rf.mu.Unlock()
	return
}

func (rf *Raft) startElections() {

	rf.mu.Lock()
	incomingIndex := rf.timeOutIndex
	numPeers := len(rf.peers)
	me := rf.me
	lengthLog := len(rf.log)
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateID = me
	args.LastLogIndex = rf.log[lengthLog-1].Index
	args.LastLogTerm = rf.log[lengthLog-1].Term
	rf.mu.Unlock()

	ch := make(chan RequestVoteReply)

	for i := 0; i < numPeers; i++ {
		if i == me {
			continue
		}

		go func(i int) {
			reply := RequestVoteReply{}
			DPrintf("%d: Sending out Request vote %v for %d\n", rf.me, args, i)
			ok := rf.sendRequestVote(i, &args, &reply)
			ch <- reply
			DPrintf("%d: Receiving Request vote reply %v from %d\n", rf.me, reply, i)

			if !ok {
				return
			}

			if reply.Term > args.Term {
				rf.mu.Lock()
				outgoingIndex := rf.timeOutIndex
				if outgoingIndex == incomingIndex {
					rf.state = 0
					rf.currentTerm = reply.Term
					rf.timeOutResult = false
					DPrintf("%d: Found higher term, Converting back to follower\n", rf.me)
					rf.mu.Unlock()
					rf.persist()
					rf.cond.Signal()
					return
				}
				rf.mu.Unlock()
				return
			}

		}(i)
	}

	DPrintf("%d: Starting to count votes...\n", rf.me)
	count := 1

	for i := 1; i < numPeers; i++ {
		reply := <-ch
		if reply.VoteGranted {
			count++
			DPrintf("%d: Increasing the vote count to %d\n", rf.me, count)
		}

		if count > numPeers/2 {
			DPrintf("%d: Stopping the counting process with %d votes\n", rf.me, count)
			break
		}
	}

	rf.mu.Lock()
	outgoingIndex := rf.timeOutIndex
	if outgoingIndex == incomingIndex && count > numPeers/2 {
		rf.state = 2
		rf.timeOutResult = false
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log)
		}
		rf.mu.Unlock()
		rf.cond.Signal()
		go rf.maintainCommitIndex()
		go rf.sendHeartBeats()
		return
	}
	rf.mu.Unlock()
	return

}

func (rf *Raft) sendHeartBeats() {

	rf.mu.Lock()
	numPeers := len(rf.peers)
	incomingIndex := rf.timeOutIndex
	me := rf.me
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for i := 0; i < numPeers; i++ {
		if i != me {
			go func(i int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{}
				args.LeaderID = me
				args.Term = currentTerm
				args.PrevLogIndex = rf.log[rf.nextIndex[i]-1].Index
				args.PrevLogTerm = rf.log[rf.nextIndex[i]-1].Term
				args.LeaderCommit = rf.commitIndex
				if rf.nextIndex[i] < len(rf.log) {
					args.Entries = rf.log[rf.nextIndex[i]:]
				}
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				DPrintf("%d: Sending out heartbeat %v to %d\n", rf.me, args, i)
				ok := rf.sendAppendEntries(i, &args, &reply)

				if !ok {
					return
				}

				rf.mu.Lock()
				outgoingIndex := rf.timeOutIndex
				if incomingIndex != outgoingIndex {
					rf.mu.Unlock()
					return
				}

				if reply.Term > args.Term {
					rf.state = 0
					rf.currentTerm = reply.Term
					rf.timeOutResult = false
					rf.mu.Unlock()
					rf.persist()
					rf.cond.Signal()
					return

				}

				if !reply.Success {
					if reply.Length > 0 {
						DPrintf("%d: See the log resolving by legth %v\n", rf.me, rf.log)
						// Conflict is due to the absence of entries
						DPrintf("%d: Directly backing up over empty space of %d from %d to %d\n", rf.me, i, rf.nextIndex[i], reply.Length)
						rf.nextIndex[i] = reply.Length
					} else {
						// Conflict is due to the presence of incorrect entries

						flag := false
						ind := 0
						for i := rf.nextIndex[i] - 1; i >= 0; i-- {
							if rf.log[i].Term == reply.XTerm {
								flag = true
								ind = i
								break
							}
						}
						DPrintf("%d: See the log resolving by value %v\n", rf.me, rf.log)
						if flag {
							rf.nextIndex[i] = ind
						} else {
							rf.nextIndex[i] = reply.XIndex
						}
						DPrintf("%d: Backing up over conflicting entries for %d to %d with XIndex: %d and XTerm: %d\n", rf.me, i, rf.nextIndex[i], reply.XIndex, reply.XTerm)
					}
				} else {
					if len(args.Entries) > 0 {
						rf.nextIndex[i] += len(args.Entries)
					}
					rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
				}
				rf.mu.Unlock()
				return
			}(i)
		}
	}

}

func (rf *Raft) maintainCommitIndex() {
	for !rf.killed() {
		rf.mu.Lock()
		indices := make(map[int]int)
		var target int = len(rf.peers) / 2
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				if rf.matchIndex[i] >= rf.commitIndex {
					indices[rf.matchIndex[i]]++

				}
			}
		}

		var keys []int
		for k := range indices {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] > keys[j] })

		for _, k := range keys {
			if indices[k]+1 > target && rf.log[k].Term == rf.currentTerm {
				oldIndex := rf.commitIndex
				rf.commitIndex = k
				if oldIndex != k {
					DPrintf("%d: Updating Leader Commit Index from %d to %d\n", rf.me, oldIndex, rf.commitIndex)
				}
				for i := oldIndex + 1; i <= rf.commitIndex; i++ {
					DPrintf("%d: Leader Applying up the channel %v \n", rf.me, rf.log[i])
					rf.applyCh <- ApplyMsg{Command: rf.log[i].Command, CommandValid: true, CommandIndex: rf.log[i].Index}
				}
				rf.lastApplied = rf.commitIndex
				break
			}
		}

		if rf.state != 2 {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}
