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
	// "fmt"
	"log"
	"math/rand"

	// "reflect"
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

// --- added by lbx ---
// enum role
const (
    INIT        uint = 0
    FOLLOWER    = 1
    CANDIDATE   = 2       
    LEADER      = 3
)
// --- added end ---

// a Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
    hbIdx       uint

    // Persistent state on all servers:
    currentTerm int
    votedFor    int
    log         []interface{}

    // Volatile state on all servers
    commitIndex int
    lastApplied int

    // Volatile state on leaders
    nextIndex   []int
    matchIndex  []int

    // Candidate Votes Collection
    majority    int
    voteCount   int
    role        uint
    // ponting to ticker thread heartbeated flag
    heartbeated *bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    term = rf.currentTerm
    isleader = (rf.role == LEADER)
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

// --- added by lbx ---
type AppendEntriesArgs struct {
    Term            int
    LeaderId        int
    PrevLogIndex    int
    PrevLogTerm     int
    Entries         []interface{}
    LeaderCommit    int
}

type AppendEntriesReply struct {
    Term            int
    Success         bool
    // Todo:
    // determine args
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    // lock 1st
    rf.mu.Lock()
    defer rf.mu.Unlock()
    // Handle rules for servers
    if args.Term > rf.currentTerm {
        DPrintf("[%d] SERVER[%d] receive advanced AERPC from SERVER[%d] with TERM[%d] step down to FOLLOWER",
                rf.currentTerm, rf.me, args.LeaderId, args.Term)
        rf.currentTerm = args.Term
        rf.role = FOLLOWER
        rf.voteCount = 0
        rf.votedFor = -1

    }
    // Handle RPC
    switch {
    case args.Term < rf.currentTerm:
        reply.Success = false
        reply.Term = rf.currentTerm
        DPrintf("[%d] SERVER[%d] REJECT AERPC from SERVER[%d] TERM[%d]\n", rf.currentTerm, rf.me, args.LeaderId, args.Term)
    // TODO: match
    default:
        switch rf.role {
        case FOLLOWER:
            // Already a follower trigger another  ticker
            DPrintf("[%d] FOLLOWER[%d] ACCEPT AERPC from SERVER[%d]\n", rf.currentTerm, rf.me, args.LeaderId)

        case CANDIDATE:
            rf.role = FOLLOWER
            DPrintf("[%d] CANDIDATE[%d] ACCEPT AERPC from SERVER[%d], exit election\n",
                rf.currentTerm, rf.me, args.LeaderId,
            )
        case LEADER:
            rf.role = FOLLOWER
            // lite debug
            // fmt.Printf("[%d] ***LEADER[%d] ACCEPT AERPC from SERVER[%d] with TERM[%d], exit election***\n",rf.currentTerm, rf.me, args.LeaderId, args.Term)
            DPrintf(
                "***[%d] LEADER[%d] ACCEPT AERPC from SERVER[%d], exit election***\n",
                rf.currentTerm, rf.me, args.LeaderId,
            )
        default:
            log.Fatalf(
                "[%d] Error: SERVER[%d] has misleading role:[%d]\n",
                rf.currentTerm, rf.me, rf.role,
            )
        }
        if rf.heartbeated == nil {
            // log.Fatalf("SERVER[%d] did not register its heartbeat flag", rf.me)
        } else {
            if rf.role == CANDIDATE {
                log.Fatalf("Error: CANDIDATE[%d] heartbeated after valid AERPC\n", rf.me)
            }
            DPrintf("[%d] FOLLOWER[%d] heartbeated", rf.currentTerm, rf.me)
            *rf.heartbeated = true
        }
        rf.heartbeated = nil
        reply.Success = true
        reply.Term = rf.currentTerm
        if rf.role != FOLLOWER {
            rf.mu.Unlock()
            log.Fatalf("SERVER[%d] A heartbeated SERVER or CANDIDATE\n", rf.me)
        }
        ech := make(chan bool)
        go rf.electionTicker(ech)

        finish, ok := <- ech
        if !ok { log.Fatalf("Error: SERVER[%d] CHANNEL crashed\n", rf.me) }
        if !finish { DPrintf("WARNING: SERVER[%d] election ticker did not finish\n", rf.me) }

    }
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) heartBeatThread() {

    idx := 0
    for {
        rf.mu.Lock()
        if rf.killed() || rf.role != LEADER {
            rf.mu.Unlock()
            break
        }

        if idx >= len(rf.peers) { idx = 0 }
        if rf.me == idx {
            idx += 1
            rf.mu.Unlock()
            continue
        }

        if rf.me == idx { log.Fatalln("LEADER try to send AERPC to itself") }
        rf.mu.Unlock()
        go func(idx int) {
            // lite debug
            // fmt.Printf("SERVER[%d] send AERPC to SERVER[%d]\n", rf.me, idx)
            rf.mu.Lock()
            DPrintf("LEADER[%d] send AERPC to SERVER[%d]\n", rf.me, idx)
            var args = AppendEntriesArgs{ rf.currentTerm, rf.me, 0, 0, make([]interface{}, 0), 0 }
            var reply = AppendEntriesReply{ -1, false }
            rf.mu.Unlock()
            ok := rf.sendAppendEntries(idx, &args, &reply)
            rf.mu.Lock()
            defer rf.mu.Unlock()
            switch {
            case !ok:
                DPrintf("LEADER[%d] CALL AERPC to SERVER[%d] failed\n", rf.me, idx)
            case reply.Success:
                // tmp do nothing
            case !reply.Success:
                // step down
                if rf.currentTerm < reply.Term {
                    DPrintf(
                        "[%d] LEADER[%d] receive a advanced AERPC, step back to FOLLOWER with new term[%d]",
                        rf.currentTerm, rf.me, reply.Term)
                    rf.currentTerm = reply.Term
                    rf.role = FOLLOWER
                    rf.voteCount = 0
                    rf.votedFor = -1
                } else {
                    DPrintf(
                        "[%d] LEADER[%d] Get a Misleading false AERPC from SERVER[%d], reply TERM[%d]\n",
                        rf.currentTerm, rf.me, idx, reply.Term,
                    )
                }
            }

        }(idx)
        idx += 1

        time.Sleep(100 * time.Millisecond)
    }
    DPrintf("HEARTBEAT SEND thread down\n")
}

// --- added end ---


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term            int
    CandidateId     int
    LastLogIndex    int
    LastLogTerm     int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
    Term        int
    VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    // lock 1st
    rf.mu.Lock()
    defer rf.mu.Unlock()
    // handle rules for servers
    if rf.currentTerm < args.Term {
        DPrintf(
            "[%d] SERVER[%d] receive a advanced RVRPC, step back to FOLLOWER with new term[%d]",
            rf.currentTerm, rf.me, args.Term)
        rf.currentTerm = args.Term
        rf.role = FOLLOWER
        rf.voteCount = 0
        rf.votedFor = -1
    }

    // handle RVRPC
    switch {
    case rf.currentTerm > args.Term:
        // stale RPC just reject
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
        DPrintf("SERVER[%d] REJECT vote request from SERVER[%d]: stale RPC\n", rf.me, args.CandidateId)
    case (rf.votedFor < 0 || rf.votedFor == args.CandidateId):
        rf.votedFor = args.CandidateId
        reply.Term = rf.currentTerm
        // rf.currentTerm = args.Term
        reply.VoteGranted = true
        // *rf.heartbeated = true
        // rf.heartbeated = nil
        // ech := make(chan bool)
        // go rf.electionTicker(ech)
        // finish, ok := <- ech
        // if !ok { log.Fatalf("Error: SERVER[%d] CHANNEL crashed\n", rf.me) }
        // if !finish { DPrintf("WARNING: SERVER[%d] election ticker did not finish\n", rf.me) }


        DPrintf("[%d] SERVER[%d] ACCEPT vote request from SERVER[%d]\n",
                rf.currentTerm, rf.me, args.CandidateId)
    default:
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
        DPrintf("SERVER[%d] REJECT vote request from SERVER[%d]: voted?\n", rf.me, args.CandidateId)
    }

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
// the labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// call() returns false. Thus Call() may not return for a while.
// a false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// call() is guaranteed to return (perhaps after a delay) *except* if the
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

// --- added by lbx ---
func (rf *Raft) electionTicker(c chan bool) {
    heartbeated := false
    hbIdx := rf.hbIdx
    rf.hbIdx += 1
    if rf.heartbeated != nil { log.Fatalln("Error: prev thread does not clear heartbeated flag") }
    rf.heartbeated = &heartbeated
    if heartbeated { log.Fatalln("Error: heartbeated before sleep in ticker") }
    c <- true

    ms := 500 + (rand.Int63() % 300)
    time.Sleep(time.Duration(ms) * time.Millisecond)
    rf.mu.Lock()
    DPrintf(
        "[%d] SERVER[%d] state[%d] heartbeated?%t in round[%d]\n",
        rf.currentTerm, rf.me, rf.role, heartbeated, hbIdx,
    )
    if rf.killed() {
        rf.mu.Unlock()
        return
    }
    if heartbeated {
        rf.mu.Unlock()
        return
    }
    switch rf.role {
    case LEADER:
        // Leader does not need ticker
        rf.mu.Unlock()
        return
    case CANDIDATE:
        if heartbeated {
            rf.mu.Unlock()
            // return
            log.Fatalf("Error: A heartbeated CANDIDATE[%d]", rf.me)
        }
        DPrintf("CANDIDATE[%d] rise re-election", rf.me)
        rf.riseElection()
        return
    case FOLLOWER:
        if heartbeated {
            rf.mu.Unlock()
            return
        }
        DPrintf("FOLLOWER[%d] timeout rise election", rf.me)
        rf.riseElection()
        return
    }
}
    
func (rf *Raft) riseElection() {
    ech := make(chan bool)
    rf.currentTerm += 1
    rf.role = CANDIDATE
    DPrintf("[%d] SERVER[%d] become to CANDIDATE", rf.currentTerm, rf.me)
    rf.voteCount = 1
    rf.votedFor = rf.me
    rf.heartbeated = nil
    go rf.electionTicker(ech)
    finish, ok := <- ech
    if !ok { log.Fatalf("Error: SERVER[%d] CHANNEL crashed\n", rf.me) }
    if !finish { DPrintf("WARNING: SERVER[%d] election ticker did not finish\n", rf.me) }
    rf.mu.Unlock()

    // send RVRPC to each server
    rf.mu.Lock()
    for idx, _ := range rf.peers {
        if rf.killed() {
            DPrintf("[%d] SERVER[%d] exit election: SERVER DOWN", rf.currentTerm, rf.me)
            break
        }
        if idx == rf.me { continue }
        if rf.role == FOLLOWER { break }
        rf.mu.Unlock()
        go func(idx int) {
            rf.mu.Lock()
            var args = RequestVoteArgs{ rf.currentTerm, rf.me, 0, 0 }
            rf.mu.Unlock()
            var reply = RequestVoteReply{ -1, false }
            ok := rf.sendRequestVote(idx, &args, &reply)
            rf.mu.Lock()
            defer rf.mu.Unlock()
            if ok && reply.VoteGranted {
                rf.voteCount += 1
                // got more votes than majority
                switch {
                case rf.role == LEADER:
                    // already a leader, do nothing
                case rf.voteCount >= rf.majority:
                    // collect enough votes, become to leader
                    // just in case 
                    if rf.role == FOLLOWER {
                        log.Fatalf("Error: A FOLLOWER[%d] wants to become a leader\n", rf.me)
                    }
                    rf.role = LEADER
                    // lite debug
                    // fmt.Printf("[%d] SERVER[%d] become to LEADER\n", rf.currentTerm, rf.me)
                    DPrintf("SERVER[%d] become to LEADER\n", rf.me)
                    // trigger heartbeat
                    go rf.heartBeatThread()
                default:
                    // not enough votes, exit
                }
            } else {
                switch {
                case !ok:
                    DPrintf(
                        "[%d] WARNING: SERVER[%d] can not call RV to SERVER[%d]\n",
                        rf.currentTerm, rf.me, idx,
                    )
                case !reply.VoteGranted:
                    DPrintf(
                        "[%d] SERVER[%d] REJECT RVRPC from SERVER[%d]\n",
                        rf.currentTerm, idx, rf.me,
                    )
                    if rf.currentTerm < reply.Term {
                        DPrintf(
                            "[%d] SERVER[%d] receive a advanced RVRPC, step back to FOLLOWER with new term[%d]",
                            rf.currentTerm, rf.me, reply.Term)
                        rf.currentTerm = reply.Term
                        rf.role = FOLLOWER
                        rf.voteCount = 0
                        rf.votedFor = -1
                    }

                default:
                    DPrintf(
                        "[%d] SERVER[%d] WARNING: A not ok and not granted RVRPC to SERVER[%d]\n",
                        rf.currentTerm, rf.me, idx,
                    )
                }
            }
        }(idx)
        rf.mu.Lock()
    }
    rf.mu.Unlock()
}

    


// --- added end ---


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
    DPrintf("*** SERVER[%d] killed ***", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {

	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
        rf.mu.Lock()
        switch rf.role {
        case INIT:
            // if it's the 1st round, just sleep
            DPrintf("[%d] SERVER[%d] INIT\n", rf.currentTerm, rf.me)
            rf.mu.Unlock()
        case FOLLOWER:
            // have received valid RPC, RPC will trigger ticker, just return
            rf.mu.Unlock()
            return
        case CANDIDATE:
            // no valid RPC received, rise a election
            rf.riseElection()
            return

        case LEADER:
            // a sever should not become to a LEADER after the 1st round, panic
            rf.mu.Unlock()
            log.Fatalf("Error: A LEADER[%d] appears after 1st round\n", rf.me)
        }

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

        rf.mu.Lock()
        if rf.role == INIT { rf.role = CANDIDATE }
        rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
    // rf.initTerm     = true
    rf.currentTerm  = 0
    rf.votedFor     = -1
    rf.majority     = len(rf.peers) / 2 + 1
    rf.voteCount    = 0
    rf.role         = INIT
    // a ticker theard should register its own heartbeat flag
    // an heartbeat rpc should set old flag as true and re-register flag
    rf.heartbeated  = nil
    rf.hbIdx        = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
