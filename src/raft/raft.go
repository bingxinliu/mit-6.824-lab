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
	"bytes"
	"fmt"
	"log"
	"math/rand"

	// "reflect"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
    FOLLOWER    uint = 0
    CANDIDATE   = 1       
    LEADER      = 2
)

type Entry struct {
    Term    int
    Command interface{}
}
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
    applyCh     chan ApplyMsg

    // helper index pointing to the last log's index
    currentIndex int

    // init term flag ticker implementation is hard to understand so we have this flag to
    // discriminate the situation
    initTerm    bool

    // option
    hbIdx       uint
    aerpcIdx    uint

    // Persistent state on all servers:
    currentTerm int
    votedFor    int
    log         []Entry

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

    // offset for snapshot
    lastIncludedIndex   int
    lastIncludedTerm    int
    // Snapshot
    snapShot   *[]byte 
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
    buffer := new(bytes.Buffer)
    e := labgob.NewEncoder(buffer)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.log)
    e.Encode(rf.lastIncludedIndex)
    e.Encode(rf.lastIncludedTerm)
    raftstate := buffer.Bytes()
    if rf.snapShot == nil {
        rf.persister.Save(raftstate, nil)
    } else {
        rf.persister.Save(raftstate, *rf.snapShot)
    }
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
    readBuffer := bytes.NewBuffer(data)
    d := labgob.NewDecoder(readBuffer)
    var currentTerm int
    var votedFor int
    var rflog []Entry
    var lastIncludedIndex int
    var lastIncludedTerm int
    // var snapShot []byte
    if d.Decode(&currentTerm) != nil || 
        d.Decode(&votedFor) != nil || 
        d.Decode(&rflog) != nil ||
        d.Decode(&lastIncludedIndex) != nil ||
        d.Decode(&lastIncludedTerm) != nil {
        log.Fatalf("Error: Decode problem\n")
    } else {
        rf.mu.Lock()
        defer rf.mu.Unlock()
        rf.currentTerm = currentTerm
        rf.votedFor = votedFor
        rf.log = rflog
        rf.lastIncludedIndex = lastIncludedIndex
        rf.lastIncludedTerm = lastIncludedTerm
        // rf.snapShot = snapShot
        // recover some other index
        rf.currentIndex = rf.lastIncludedIndex + len(rf.log)
        rf.lastApplied = rf.lastIncludedIndex
        rf.commitIndex = rf.lastIncludedIndex
    }
}

// --- added by lbx ---
func (rf *Raft) ReadSnapshot(data []byte){}
func (rf *Raft) assert(condition bool, message string) {
    if !condition {
        panic(fmt.Sprintf("Error: SVR[%d] %s\n", rf.me, message))
    }
}

func (rf *Raft) ApplyCommand(applyCh chan ApplyMsg, command interface{}, commandIndex int) {

}
// --- added end ---


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
    // a wrapper for fixing stupid test design
    DbgPrintf(dSnap,
        "[%d] SVR %d Snapshot for idx=%d\n",
        rf.currentTerm, rf.me, index,
    )
    go func(index int, snapshot []byte) {
        rf.mu.Lock()
        defer rf.mu.Unlock()
        if index >= len(rf.log) + rf.lastIncludedIndex + 1 {
            // can not trim b/c log too short
            DbgPrintf(dInfo,
                "[%d] SVR %d Snapshot() called failed, too short log length. idx:%d, len:%d\n",
                rf.currentTerm, rf.me, index, len(rf.log),
            )
            return
        }

        // TODO: not necessary
        if index <= rf.lastIncludedIndex {
            // log.Fatalf("Error Snapshot Conflict\n")
            return
        }
        rf.lastIncludedTerm = rf.log[index-rf.lastIncludedIndex-1].Term
        rf.log = rf.log[index-rf.lastIncludedIndex:]
        rf.lastIncludedIndex = index
        rf.snapShot = &snapshot
        DbgPrintf(dSnap,
            "[%d] SVR %d trimed, LII:%d len(log)=%d\n",
            rf.currentTerm, rf.me, rf.lastIncludedIndex, len(rf.log),
        )
        // fixing stupid ingestSnap design
        if rf.lastApplied < rf.lastIncludedIndex {
            rf.lastApplied = rf.lastIncludedIndex
        }
        if rf.commitIndex < rf.lastIncludedIndex {
            rf.commitIndex = rf.lastIncludedIndex
        }
        if rf.currentIndex < rf.lastIncludedIndex {
            rf.currentIndex = rf.lastIncludedIndex
        }
        rf.persist()
        // applyMsg := ApplyMsg{
        //     CommandValid    : false,
        //     Command         : nil,
        //     CommandIndex    : -1,
        //     SnapshotValid   : true,
        //     Snapshot        : *rf.snapShot,
        //     SnapshotTerm    : rf.lastIncludedTerm,
        //     SnapshotIndex   : rf.lastIncludedIndex,
        // }
        // rf.applyCh <- applyMsg
        DbgPrintf(dSnap,
            "[%d] SVR %d Snapshot() called idx:%d\n",
            rf.currentTerm, rf.me, index,
        )
    }(index, snapshot)

}

// --- added by lbx ---
// TODO: may templatized by reflect
func (rf *Raft) checkAllServersRules(term, leaderId int) bool {
    persistentStateModified := false
    for rf.commitIndex > rf.lastApplied {
        rf.lastApplied += 1
        applyMsg := ApplyMsg {
            CommandValid    : true,
            Command         : rf.log[rf.lastApplied-rf.lastIncludedIndex-1].Command,
            CommandIndex    : rf.lastApplied,
            SnapshotValid   : false,
            Snapshot        : nil,
            SnapshotTerm    : -1,
            SnapshotIndex   : -1,
        }
        rf.applyCh <- applyMsg
        DbgPrintf(
            dApply,
            "[%d] SVR %d apply command[%d] into SM (checkAllServersRules)\n",
            rf.currentTerm, rf.me, rf.lastApplied,
        )
    }
    if term > rf.currentTerm {
        DbgPrintf(
            dTerm,
            "[%d] SVR %d receive advanced AERPC from LDR[%d] with TERM[%d] step down to FOLLOWER\n",
            rf.currentTerm, rf.me, leaderId, term,
        )
        rf.currentTerm = term
        rf.role = FOLLOWER
        rf.voteCount = 0
        rf.votedFor = -1
        persistentStateModified = true
    }
    DbgPrintf(
        dTrace,
        "[%d] SVR %d all servers rules checked\n", rf.currentTerm, rf.me,
    )
    return persistentStateModified
}

type InstallSnapshotArgs struct {
    Term                int
    LeaderId            int
    LastIncludeIndex    int
    LastIncludeTerm     int
    // offset is not used in this lab, so as Done
    Offset              int
    Data                []byte
    Done                bool
}

type InstallSnapshotReply struct {
    Term                int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    persistentStateModified := false
    rf.mu.Lock()
    defer rf.mu.Unlock()
    DbgPrintf(
        dState,
        "[%d] SVR %d <-ISRPC- LDR[%d], [T:%d, Lid:%d, LII:%d, LIT:%d, Off:%d, Len(Data):%d, Done:%t] CurrentState[cTerm:%d, vFor:%d, len(log)=%d, lastLog:v cmtIdx:%d, lastIncluIdx:%d, lstAppliad:%d\n",
        rf.currentTerm, rf.me, args.LeaderId,
        args.Term, args.LeaderId, args.LastIncludeIndex, args.LastIncludeTerm, args.Offset, len(args.Data), args.Done,
        rf.currentTerm, rf.votedFor, len(rf.log), rf.commitIndex, rf.lastIncludedIndex, rf.lastApplied,
    )

    // Handle rules for servers
    persistentStateModified = rf.checkAllServersRules(args.Term, args.LeaderId)
    // Handle RPC
    switch {
    case args.Term < rf.currentTerm:
        reply.Term = rf.currentTerm
        DbgPrintf(dTrace,
            "[%d] SVR %d REJECT ISRPC from SVR[%d] TERM[%d]: stale ISRPC\n",
            rf.currentTerm, rf.me, args.LeaderId, args.Term,
        )
        if persistentStateModified {
            log.Fatalf("[%d] SVR %d change persistent state for a stale ISRPC\n", rf.currentTerm, rf.me)
        }
    default:
        reply.Term = rf.currentTerm
        // TODO:
        // 1. Reply immediately if term < currentTerm
        // 2. Create new snapshot file if first chunk (offset is 0)
        // 3. Write data into snapshot file at given offset
        // 4. Reply and wait for more data chunks if done is false
        // 5. Save snapshot file, discard any existing or partial snapshot with a smaller index
        switch {
        case args.LastIncludeIndex < rf.lastIncludedIndex :
            DbgPrintf(dInfo,
                "[%d] SVR %d Snapshot Conflict: args.LII:%d, rf.LII:%d drop ISRPC\n",
                rf.currentTerm, rf.me, args.LastIncludeIndex, rf.lastIncludedIndex)
            return
        case args.LastIncludeIndex == rf.lastIncludedIndex :
            DbgPrintf(dTrace,
                "[%d] SVR %d REJECT ISRPC from SVR[%d] TERM[%d]: duplicated ISRPC\n",
                rf.currentTerm, rf.me, args.LeaderId, args.Term,
            )
            return
        case rf.lastIncludedIndex + len(rf.log) > args.LastIncludeIndex:
            rf.log = rf.log[args.LastIncludeIndex-rf.lastIncludedIndex:]
        default:
            rf.log = rf.log[0:0]
            // TODO:
            // 8. Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
        }
        
        rf.lastIncludedIndex = args.LastIncludeIndex
        rf.lastIncludedTerm = args.LastIncludeTerm
        rf.snapShot = &args.Data
        // fix stupid ingestSnap design
        // if rf.lastApplied < rf.lastIncludedIndex { rf.lastApplied = rf.lastIncludedIndex }
        rf.lastApplied = rf.lastIncludedIndex
        if rf.commitIndex < rf.lastIncludedIndex { rf.commitIndex = rf.lastIncludedIndex }
        rf.currentIndex = rf.lastIncludedIndex + len(rf.log)

        applyMsg := ApplyMsg{
            CommandValid    : false,
            Command         : nil,
            CommandIndex    : -1,
            SnapshotValid   : true,
            Snapshot        : *rf.snapShot,
            SnapshotTerm    : rf.lastIncludedTerm,
            SnapshotIndex   : rf.lastIncludedIndex,
        }
        rf.applyCh <- applyMsg


        persistentStateModified = true
        DbgPrintf(dState,
            "[%d] SVR %d <-ISRPC- LDR[%d] ACCEPTED CurrentState[cTerm:%d, vFor:%d, len(log)=%d, lastLog:v cmtIdx:%d, lastIncluIdx:%d, lstAppliad:%d\n",
            rf.currentTerm, rf.me, args.LeaderId,
            rf.currentTerm, rf.votedFor, len(rf.log), rf.commitIndex, rf.lastIncludedIndex, rf.lastApplied,
        )
    }
    if persistentStateModified { rf.persist() }
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
    return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) SendAndHandleInstallSnapshotRPC(idx int) {
    var termBeforeSend int
    args := InstallSnapshotArgs{}
    reply := InstallSnapshotReply{ -1 }
    rf.mu.Lock()
    if rf.role != LEADER { return }
    termBeforeSend = rf.currentTerm
    // Init args
    args.Term = rf.currentTerm
    args.LeaderId = rf.me
    args.LastIncludeIndex = rf.lastIncludedIndex
    args.LastIncludeTerm = rf.lastIncludedTerm
    args.Offset = 0
    args.Data = *rf.snapShot
    args.Done = true
    rf.mu.Unlock()


    ok := rf.sendInstallSnapshot(idx, &args, &reply)


    persistentStateModified := false
    rf.mu.Lock()
    defer rf.mu.Unlock()

    switch {
    case rf.killed():
        return
    case rf.role != LEADER:
        DbgPrintf(dInfo,
            "[%d] SVR %d -ISRPC-> SVR[%d]: already no longer Leader, drop it\n",
            rf.currentTerm, rf.me, idx,
        )
        return
    }

    // check result
    switch {
    case !ok:
        DbgPrintf(
            dWarn, "[%d] LDR %d -ISRPC-> SVR[%d] failed\n",
            rf.currentTerm, rf.me, idx)
        // retry failed RPC
        if rf.currentTerm == termBeforeSend {
            rf.mu.Unlock()
            time.Sleep(10 * time.Millisecond)
            rf.mu.Lock()
            go rf.SendAndHandleInstallSnapshotRPC(idx)
        }
        return
    default:
        DbgPrintf(dISRPC,
            "[%d] LDR %d -ISRPC-> SVR[%d]: result get [T:%d]\n",
            rf.currentTerm, rf.me, idx, reply.Term,
        )
        termBeforeUpdate := rf.currentTerm
        if rf.currentTerm < reply.Term {
            DbgPrintf(dRole,
                "[%d] LDR %d <-advanced ISRPC- SVR[%d], a new leader exist step back to FLW with new term[%d]",
                rf.currentTerm, rf.me, idx, reply.Term,
            )
            rf.currentTerm = reply.Term
            rf.role = FOLLOWER
            rf.voteCount = 0
            rf.votedFor = -1
            persistentStateModified = true

            rf.heartbeated = nil
            ech := make(chan bool)
            go rf.electionTicker(ech)

            finish, ok := <- ech
            if !ok { log.Fatalf("Error: SERVER[%d] CHANNEL crashed\n", rf.me) }
            if !finish {
                DbgPrintf(
                    dWarn,
                    "[%d] SVR %d election ticker did not finish\n", rf.currentTerm, rf.me)
            }
        }

        if termBeforeUpdate > termBeforeSend {
            DbgPrintf(dInfo,
            "[%d] SVR %d -ISRPC-> SVR[%d]: sendTerm:%d, resultTerm:%d RPC result get: old term, drop it\n",
                rf.currentTerm, rf.me, idx, termBeforeSend, termBeforeUpdate,
            )
        } else {
            DbgPrintf(dInfo,
            "[%d] SVR %d -ISRPC-> SVR[%d]: result accepted\n",
                rf.currentTerm, rf.me, idx,
            )
        }

        if persistentStateModified { rf.persist() }
        return
    }
}

type AppendEntriesArgs struct {
    Term            int
    LeaderId        int
    PrevLogIndex    int
    PrevLogTerm     int
    Entries         []Entry
    LeaderCommit    int
}

type AppendEntriesReply struct {
    // term in the conflicting entry (if any)
    XTerm           int
    // index of the 1st entry with that term (if any)
    XIndex          int
    // log length
    XLen            int
    Term            int
    Success         bool
    // Todo:
    // determine args
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    persistentStateModified := false
    // lock 1st
    rf.mu.Lock()
    defer rf.mu.Unlock()
    DbgPrintf(
        dState,
        "[%d] SVR %d <-AERPC- LDR[%d], [T:%d, PrevLI:%d, PrevLT:%d, len(Entries)=%d, Lcmt:%d] CurrentState[cTerm:%d, vFor:%d, cIdx:%d len(log)=%d, cmtIdx:%d, lastIncluIdx:%d, lstAppliad:%d\n",
        rf.currentTerm, rf.me, args.LeaderId, 
        args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit,
        rf.currentTerm, rf.votedFor, rf.currentIndex, len(rf.log), rf.commitIndex,
        rf.lastIncludedIndex, rf.lastApplied,
    )
    // Handle rules for servers
    persistentStateModified = rf.checkAllServersRules(args.Term, args.LeaderId)
    // Handle RPC
    switch {
    case args.Term < rf.currentTerm:
        reply.Success = false
        reply.Term = rf.currentTerm
        DbgPrintf(
            dTrace,
            "[%d] SVR %d REJECT AERPC from SVR[%d] TERM[%d]: stale AERPC\n",
            rf.currentTerm, rf.me, args.LeaderId, args.Term,
        )
        if persistentStateModified {
            log.Fatalf("[%d] SVR %d change persistent state for a stale AERPC\n", rf.currentTerm, rf.me)
        }
    case ((
        rf.currentIndex < args.PrevLogIndex ) || (
        args.PrevLogIndex < rf.lastIncludedIndex ) || (
        args.PrevLogIndex == rf.lastIncludedIndex && args.PrevLogTerm != rf.lastIncludedTerm ) || (
        args.PrevLogIndex > rf.lastIncludedIndex && rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term != args.PrevLogTerm)):
        switch {
        case rf.currentIndex < args.PrevLogIndex:
            // follower's log is too short:
            reply.XLen = rf.currentIndex
            DbgPrintf(dConsist, "[%d] SVR %d too short log( %d )\n",
                rf.currentTerm, rf.me, rf.currentIndex,
            )
        case args.PrevLogIndex < rf.lastIncludedIndex:
            // server does not have the log to which the prevlogIndex points.
            // or server has trimmed the log
            reply.XIndex = rf.currentIndex
            DbgPrintf(dConsist, "[%d] SVR %d the log has been truncated lastIncludedIndex:%d\n",
                rf.currentTerm, rf.me, rf.lastIncludedIndex,
            )
        case args.PrevLogIndex == rf.lastIncludedIndex && args.PrevLogTerm != rf.lastIncludedTerm:
            // Leader conflict with a commited log, this should never happenned
            log.Fatalf("Error: Commit Conflict PLI=LII=%d, but PLT=%d LIT=%d",
                rf.lastIncludedIndex, args.PrevLogTerm, rf.lastIncludedTerm,
            )
        default:
            conflictTerm := rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term
            firstIdx := args.PrevLogIndex - rf.lastIncludedIndex - 1 - 1
            reply.XTerm = conflictTerm
            for firstIdx >= 0 {
                if rf.log[firstIdx].Term != conflictTerm { break }
                firstIdx -= 1
            }
            reply.XIndex = firstIdx + rf.lastIncludedIndex + 1 + 1
            DbgPrintf(dConsist, "[%d] SVR %d Term conflicting [T:%d, I:%d]\n",
                rf.currentTerm, rf.me, reply.XTerm, reply.XIndex,
            )
        }

        reply.Success = false

        reply.Term = rf.currentTerm
        DbgPrintf(
            dTrace,
            "[%d] SVR %d REJECT AERPC from SVR[%d] TERM[%d], consist conflicting\n",
            rf.currentTerm, rf.me, args.LeaderId, args.Term,
        )

        if rf.role != FOLLOWER {
            DbgPrintf(dRole,
                "[%d] SVR[%d] state[%d] step down to FLW\n",
                rf.currentTerm, rf.me, rf.role)
            rf.role = FOLLOWER
        }

        if rf.heartbeated == nil {
            DbgPrintf(dWarn,
                "[%d] SVR[%d] did not register its heartbeat flag",
                rf.currentTerm, rf.me)
        } else {
            DbgPrintf(
                dHBeat,
                "[%d] FLW %d  heartbeated", rf.currentTerm, rf.me)
            *rf.heartbeated = true
        }
        rf.heartbeated = nil
        if rf.role != FOLLOWER {
            rf.mu.Unlock()
            log.Fatalf("SERVER[%d] A heartbeated SERVER or CANDIDATE\n", rf.me)
        }
        ech := make(chan bool)
        go rf.electionTicker(ech)

        finish, ok := <- ech
        if !ok { log.Fatalf("Error: SERVER[%d] CHANNEL crashed\n", rf.me) }
        if !finish { DbgPrintf(
            dWarn, "[%d] SVR %d election ticker did not finish\n", rf.currentTerm, rf.me) }

        if persistentStateModified { rf.persist() }
    default:
        // at this point, server should accept AERPC, sync log[]
        nextMatchIndex := args.PrevLogIndex + 1
        entryIndex := 0
        for _, entry := range args.Entries {
            if rf.currentIndex < nextMatchIndex { break }
            if rf.log[nextMatchIndex-rf.lastIncludedIndex-1].Term != entry.Term {
                rf.log = rf.log[:nextMatchIndex-rf.lastIncludedIndex-1]
                rf.currentIndex = nextMatchIndex - 1
                persistentStateModified = true
                break
            }
            entryIndex += 1
            nextMatchIndex += 1
        }
        for entryIndex < len(args.Entries) {
            rf.log = append(rf.log, args.Entries[entryIndex])
            persistentStateModified = true
            entryIndex += 1
            nextMatchIndex += 1
            rf.currentIndex += 1
        }

        if args.LeaderCommit > rf.commitIndex {
            if args.LeaderCommit > nextMatchIndex - 1 {
                rf.commitIndex = rf.lastApplied
            } else {
                rf.commitIndex = args.LeaderCommit
            }
        }
        DbgPrintf(
            dConsist,
            "[%d] SVR %d Finish log upodate len(log)=%d !\n", rf.currentTerm, rf.me, len(rf.log))

        switch rf.role {
        case FOLLOWER:
            // Already a follower trigger another  ticker
            DbgPrintf(
                dTrace,
                "[%d] FLW %d ACCEPT AERPC from LDR[%d]\n",
                rf.currentTerm, rf.me, args.LeaderId,
            )

        case CANDIDATE:
            rf.role = FOLLOWER
            DbgPrintf(
                dRole,
                "[%d] CAN %d ACCEPT AERPC from LDR[%d], exit election\n",
                rf.currentTerm, rf.me, args.LeaderId,
            )
        case LEADER:
            rf.role = FOLLOWER
            DbgPrintf(
                dRole,
                "[%d] LDR %d ACCEPT AERPC from new LDR[%d], exit election\n",
                rf.currentTerm, rf.me, args.LeaderId,
            )
        default:
            log.Fatalf(
                "[%d] Error: SERVER[%d] has misleading role:[%d]\n",
                rf.currentTerm, rf.me, rf.role,
            )
        }
        if rf.heartbeated == nil {
            DbgPrintf(dWarn,
                "[%d] SERVER[%d] did not register its heartbeat flag",
                rf.currentTerm, rf.me)
        } else {
            if rf.role == CANDIDATE {
                log.Fatalf("Error: CANDIDATE[%d] heartbeated after valid AERPC\n", rf.me)
            }
            DbgPrintf(
                dHBeat,
                "[%d] FLW %d  heartbeated", rf.currentTerm, rf.me)
            *rf.heartbeated = true
        }
        rf.heartbeated = nil
        if rf.role != FOLLOWER {
            rf.mu.Unlock()
            log.Fatalf("SERVER[%d] A heartbeated SERVER or CANDIDATE\n", rf.me)
        }
        ech := make(chan bool)
        go rf.electionTicker(ech)

        finish, ok := <- ech
        if !ok { log.Fatalf("Error: SERVER[%d] CHANNEL crashed\n", rf.me) }
        if !finish { DbgPrintf(
            dWarn, "[%d] SVR %d election ticker did not finish\n", rf.currentTerm, rf.me) }
        
        // check again for commitIndex
        for rf.commitIndex > rf.lastApplied {
            rf.lastApplied += 1
            applyMsg := ApplyMsg {
                CommandValid    : true,
                Command         : rf.log[rf.lastApplied-rf.lastIncludedIndex-1].Command,
                CommandIndex    : rf.lastApplied,
                SnapshotValid   : false,
                Snapshot        : nil,
                SnapshotTerm    : -1,
                SnapshotIndex   : -1,
            }
            rf.applyCh <- applyMsg
            DbgPrintf(
                dApply,
                "[%d] SVR %d apply command[%d] into SM (AERPC)\n",
                rf.currentTerm, rf.me, rf.lastApplied,
            )
        }

        if persistentStateModified { rf.persist() }
        reply.Success = true
        reply.Term = rf.currentTerm

        DbgPrintf(
            dState,
            "[%d] SVR %d State|cTerm:%d, vFor:%d, len(log)=%d, cmtIdx:%d, lstAply:%d, lastIncluIdx:%d|\n",
            rf.currentTerm, rf.me, rf.currentTerm, rf.votedFor, len(rf.log),
            rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex,
        )
    }

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) SendAndHandleAppendEntriesRPC(idx int, heartbeatAERPC bool) {
    // lite debug
    // fmt.Printf("SERVER[%d] send AERPC to SERVER[%d]\n", rf.me, idx)
    rf.mu.Lock()
    if rf.role != LEADER {
        rf.mu.Unlock()
        return
    }
    termBeforeSend := rf.currentTerm
    aerpcIdx := rf.aerpcIdx
    rf.aerpcIdx += 1
    if rf.nextIndex[idx] > rf.currentIndex + 1 {
        log.Fatalf("FAIL: nextIdx[%d]:%d, len(rf.log):%d, lastIncluIdx:%d\n", idx, rf.nextIndex[idx], len(rf.log), rf.lastIncludedIndex)
    }

    DbgPrintf(dInfo,
        "[%d] SVR %d |len(log)=%d, nIdx[%d]=%d, lastIncluIdx=%d|\n",
        rf.currentTerm, rf.me, len(rf.log), idx, rf.nextIndex[idx], rf.lastIncludedIndex,
    )
    args := AppendEntriesArgs{}
    args.Term           = rf.currentTerm
    args.LeaderId       = rf.me
    args.LeaderCommit   = rf.commitIndex
    // prevlogIndex, prevlogterm, entries depends:
    if rf.nextIndex[idx] <= rf.lastIncludedIndex + 1 {
        // nextIndex has been truncated, just send log currently exist
        args.PrevLogIndex = rf.lastIncludedIndex
        args.PrevLogTerm = rf.lastIncludedTerm
    } else {
        // nextIndex still exist
        args.PrevLogIndex = rf.nextIndex[idx] - 1
        args.PrevLogTerm = rf.log[rf.nextIndex[idx]-rf.lastIncludedIndex-2].Term
    }
    // then determine entries
    if heartbeatAERPC {
        args.Entries = rf.log[0:0]
    } else {
        args.Entries = rf.log[args.PrevLogIndex-rf.lastIncludedIndex:]
    }
    var reply = AppendEntriesReply{ -1, -1, -1, -1, false }
    DbgPrintf(
        dAERPC, "[%d] LDR %d -AERPC[%d]-> SVR[%d] [T:%d, LdrId:%d, PrvLI:%d, PrvLT:%d, len(Entries)=%d, LdrCmt:%d]\n",
        rf.currentTerm, rf.me, aerpcIdx, idx, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm,
        len(args.Entries), args.LeaderCommit,
    )
    rf.mu.Unlock()



    ok := rf.sendAppendEntries(idx, &args, &reply)

    

    rf.mu.Lock()
    persistentStateModified := false
    defer rf.mu.Unlock()
    if rf.killed() { return }
    // if failed to send or ger result just return
    if !ok {
        DbgPrintf(
            dWarn, "[%d] LDR %d -AERPC[%d]-> SVR[%d] failed\n",
            rf.currentTerm, rf.me, aerpcIdx, idx)
        // retry failed RPC
        if !heartbeatAERPC && rf.role == LEADER && rf.currentTerm == termBeforeSend {
            rf.mu.Unlock()
            time.Sleep(10 * time.Millisecond)
            rf.mu.Lock()
            go rf.SendAndHandleAppendEntriesRPC(idx, false)
        }
        return
    }

    DbgPrintf(
        dAERPC,
        "[%d] LDR %d -AERPC[%d]-> SVR[%d]: result get [XT:%d, XI:%d, XL:%d, T:%d, S:%t]\n",
        rf.currentTerm, rf.me, aerpcIdx, idx, reply.XTerm, reply.XIndex, reply.XLen, reply.Term,
        reply.Success,
    )

    // check term 1st
    termBeforeUpdate := rf.currentTerm
    if rf.currentTerm < reply.Term {
        DbgPrintf(
            dRole,
            "[%d] LDR %d <-advanced AERPC- SVR[%d], a new leader exist step back to FLW with new term[%d]",
            rf.currentTerm, rf.me, idx, reply.Term,
        )
        rf.currentTerm = reply.Term
        rf.role = FOLLOWER
        rf.voteCount = 0
        rf.votedFor = -1
        persistentStateModified = true
        // reset timer
        rf.heartbeated = nil
        ech := make(chan bool)
        go rf.electionTicker(ech)

        finish, ok := <- ech
        if !ok { log.Fatalf("Error: SERVER[%d] CHANNEL crashed\n", rf.me) }
        if !finish {
            DbgPrintf(
                dWarn,
                "[%d] SVR %d election ticker did not finish\n", rf.currentTerm, rf.me)
        }
    }
    // check if the rpc stale
    if termBeforeUpdate > termBeforeSend {
        DbgPrintf(
            dInfo, "[%d] SVR %d -AERPC-> SVR[%d]: sendTerm:%d, resultTerm:%d RPC result get: old term, drop it\n",
            rf.currentTerm, rf.me, idx, termBeforeSend, termBeforeUpdate,
        )
        if persistentStateModified { rf.persist() }
        return
    }
    // just in case:
    if rf.role != LEADER {
        DbgPrintf(
            dInfo, "[%d] SVR %d -AERPC-> SVR[%d]: already no longer Leader, drop it\n",
            rf.currentTerm, rf.me, idx,
        )
        if persistentStateModified { rf.persist() }
        return
    }

    // check the result:
    switch {
    case reply.Success:
        rf.nextIndex[idx] = args.PrevLogIndex + len(args.Entries) + 1
        rf.matchIndex[idx] = args.PrevLogIndex + len(args.Entries)
        // check commit status
        i := rf.commitIndex - rf.lastIncludedIndex
        commitIndexChanged := false
        for ; i < len(rf.log); i++ {
            count := 1
            for peerIdx := range rf.peers {
                if rf.matchIndex[peerIdx] >= i + rf.lastIncludedIndex + 1 { count += 1 }
            }
            if  count >= rf.majority && rf.log[i].Term == rf.currentTerm {
                rf.commitIndex = i + rf.lastIncludedIndex + 1
                commitIndexChanged = true
            }
        }
        // sync for commitIndx
        if commitIndexChanged {
            go rf.Sync()
        }

        // send committed entry to SM and applyCh
        for rf.commitIndex > rf.lastApplied {
            rf.lastApplied += 1
            applyMsg := ApplyMsg {
                CommandValid    : true,
                Command         : rf.log[rf.lastApplied-rf.lastIncludedIndex-1].Command,
                CommandIndex    : rf.lastApplied,
                SnapshotValid   : false,
                Snapshot        : nil,
                SnapshotTerm    : -1,
                SnapshotIndex   : -1,
            }
            rf.applyCh <- applyMsg
            DbgPrintf(
                dApply, "[%d] LDR %d apply [%d] to SM (handle AERPC)\n", 
                rf.currentTerm, rf.me, rf.lastApplied,
            )
        }
            
        // check new logs
        if len(rf.log) + rf.lastIncludedIndex >= rf.nextIndex[idx] {
            go rf.SendAndHandleAppendEntriesRPC(idx, false)
        }
        DbgPrintf(
            dAERPC,
            "[%d] LDR %d <-AERPC- SVR[%d] accepted, nextIdx[%d], matchIdx[%d]\n",
            rf.currentTerm, rf.me, idx, rf.nextIndex[idx], rf.matchIndex[idx],
        )

    case !reply.Success:
        switch {
        case termBeforeSend < reply.Term:
            // caused by term step down
            DbgPrintf(
                dRole,
                "[%d] SVR %d -AERPC[%d]-> SVR[%d], result get, REJECTED for TERM[%d]",
                rf.currentTerm, rf.me, aerpcIdx, idx, reply.Term,
            )
        default:
            // caused by log inconsistency
            switch {
            case reply.XLen == -1:
                lastEntryIdx := -1
                for i := args.PrevLogIndex - rf.lastIncludedIndex - 2; i >= 0; i-- {
                    if rf.log[i].Term == reply.XTerm {
                        lastEntryIdx = i + rf.lastIncludedIndex + 1
                    }
                }
                if lastEntryIdx >= 0 {
                    rf.nextIndex[idx] = lastEntryIdx
                } else {
                    rf.nextIndex[idx] = reply.XIndex
                }
                if rf.nextIndex[idx] - 1 < rf.lastIncludedIndex && rf.snapShot != nil {
                    DbgPrintf(dConsist,
                        "[%d] LDR %d  Get a lost SVR[%d], nextIdx[%d] XLen:%d LII:%d\n",
                        rf.currentTerm, rf.me, idx,
                        rf.nextIndex[idx], reply.XLen, rf.lastIncludedIndex,
                    )
                    // server does not catch up, we should send it with snapshot
                    // TODO: call install snapshot rpc
                    go rf.SendAndHandleInstallSnapshotRPC(idx)
                    return
                }
            default:
                // follower's log is too short
                rf.nextIndex[idx] = reply.XLen
                if reply.XLen - 1 < rf.lastIncludedIndex && rf.snapShot != nil {
                    DbgPrintf(dConsist,
                        "[%d] LDR %d  Get a lost SVR[%d], nextIdx[%d] XLen:%d LII:%d\n",
                        rf.currentTerm, rf.me, idx,
                        rf.nextIndex[idx], reply.XLen, rf.lastIncludedIndex,
                    )
                    // server does not catch up, we should send it with snapshot
                    // TODO: call install snapshot rpc
                    go rf.SendAndHandleInstallSnapshotRPC(idx)
                    return
                }
            }
            DbgPrintf(
                dConsist,
                "[%d] LDR %d  Get a log inconsistency AERPC from SVR[%d], nextIdx[%d]\n",
                rf.currentTerm, rf.me, idx, rf.nextIndex[idx],
            )
            // retry
            go rf.SendAndHandleAppendEntriesRPC(idx, false)
        }
    }
    if persistentStateModified { rf.persist() }
}

func (rf *Raft) heartBeatThread() {
    idx := 0
    for {
        rf.mu.Lock()
        if rf.killed() || rf.role != LEADER {
            rf.mu.Unlock()
            break
        }

        if idx >= len(rf.peers) {
            idx = 0
            if len(rf.peers) <= 10 {
                rf.mu.Unlock()
                time.Sleep(time.Duration(1000 / (10 / (len(rf.peers) - 1))) * time.Millisecond)
                rf.mu.Lock()
            } else {
                log.Fatalln("Current does not support more than 10 servers")
            }
        }
        if rf.me == idx {
            idx += 1
            rf.mu.Unlock()
            continue
        }

        if rf.me == idx { log.Fatalln("LEADER try to send AERPC to itself") }
        rf.mu.Unlock()

        go rf.SendAndHandleAppendEntriesRPC(idx, true)
        idx += 1

        // time.Sleep(100 * time.Millisecond)
    }
    DbgPrintf(dHBeat, "[%d] LDR %d  HB thread down\n", rf.currentTerm, rf.me)
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
    persistentStateModified := false
    tickerShouldReset := false
    // lock 1st
    rf.mu.Lock()
    defer rf.mu.Unlock()
    // handle rules for servers
    DbgPrintf(dRVRPC,
        "[%d] SVR %d  <-RVRPC- SVR[%d], [T:%d, LastLI:%d, LastLT:%d] CurrentState[cTerm:%d, vFor:%d, currentIdx:%d, len(log)=%d, cmtIdx:%d, lstAppliad:%d, lstIncIdx:%d\n",
        rf.currentTerm, rf.me, args.CandidateId,
        args.Term, args.LastLogIndex, args.LastLogTerm,
        rf.currentTerm, rf.votedFor, rf.currentIndex, len(rf.log), rf.commitIndex,
        rf.lastApplied, rf.lastIncludedIndex,
    )
    var role string
    switch rf.role {
    case FOLLOWER:
        role = "FLW"
    case CANDIDATE:
        role = "CAN"
    case LEADER:
        role = "LDR"
    }
    if rf.currentTerm < args.Term {
        DbgPrintf(
            dRole,
            "[%d] %s %d  <-advanced RVRPC- SVR[%d], step back to FOLLOWER with new term[%d]",
            rf.currentTerm, role, rf.me, args.CandidateId, args.Term)
        rf.currentTerm = args.Term
        if rf.role == LEADER {
            tickerShouldReset = true
        }
        role, rf.role = "FLW", FOLLOWER
        rf.voteCount = 0
        rf.votedFor = -1
        persistentStateModified = true
    }

    // handle RVRPC
    switch {
    case rf.currentTerm > args.Term:
        // stale RPC just reject
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
        DbgPrintf(
            dRVRPC, "[%d] %s %d  <-RVRPC- SVR[%d] REJECTED: stale RPC\n",
            rf.currentTerm, role, rf.me, args.CandidateId)
        if persistentStateModified {
            log.Fatalf("[%d] SVR  %d change persistent state for a stale RVRPC\n", rf.currentTerm, rf.me)
        }
    case (rf.votedFor < 0 || rf.votedFor == args.CandidateId):
        var lastTerm int
        if rf.currentIndex < rf.lastIncludedIndex {
            log.Fatalf("Error: SVR[%d] currentIndex(%d) < LastIncludeIndex(%d)",
                rf.me, rf.currentIndex, rf.lastIncludedIndex,
            )
        } else if rf.currentIndex == rf.lastIncludedIndex {
            // just in case
            if len(rf.log) != 0 {
                log.Fatalf("Error: SVR[%d] currentIndex = LastIncludeIndex(%d), but len(log)=%d",
                    rf.me, rf.lastIncludedIndex, len(rf.log),
                )
            }
            lastTerm = rf.lastIncludedTerm
        } else {
            // just in case
            if len(rf.log) != rf.currentIndex - rf.lastIncludedIndex {
                log.Fatalf("Error: SVR[%d] idx conflict, cIdx(%d) LII(%d) len(log)=%d\n",
                    rf.me, rf.currentIndex, rf.lastIncludedIndex, len(rf.log),
                )
            }
            lastTerm = rf.log[len(rf.log)-1].Term
        }
        if lastTerm <= args.LastLogTerm {
            if lastTerm < args.LastLogTerm || rf.currentIndex <= args.LastLogIndex {
                rf.votedFor = args.CandidateId
                persistentStateModified = true
                reply.VoteGranted = true
                DbgPrintf(dVote, "[%d] %s %d  <-RVRPC- SVR[%d] ACCEPTED\n",
                    rf.currentTerm, role, rf.me, args.CandidateId)

                // vote granted, reset timer
                *rf.heartbeated = true
                rf.heartbeated = nil
                // just in case
                if persistentStateModified { rf.persist() }
                reply.Term = rf.currentTerm
                ech := make(chan bool)
                go rf.electionTicker(ech)

                finish, ok := <- ech
                if !ok { log.Fatalf("Error: SERVER[%d] CHANNEL crashed\n", rf.me) }
                if !finish {
                    DbgPrintf(
                        dWarn,
                        "[%d] SVR %d  election ticker did not finish\n", rf.currentTerm, rf.me)
                }
                return
            }
        }
        fallthrough
    default:
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
        if rf.currentIndex < rf.lastIncludedIndex {
            log.Fatalf("Error: SVR[%d] currentIndex(%d) < LastIncludeIndex(%d)",
                rf.me, rf.currentIndex, rf.lastIncludedIndex,
            )
        } else if rf.currentIndex == rf.lastIncludedIndex {
            rf.assert(len(rf.log) == 0, fmt.Sprintf(
                "CI=LII=%d, but len(log)=%d", rf.currentIndex, len(rf.log),
            ))
            DbgPrintf(dRVRPC,
                "[%d] SVR %d <-RVRPC- SVR[%d] REJECT: votedfor[%d] lastLog|idx: %d, term: %d|\n",
                rf.currentTerm, rf.me, args.CandidateId,
                rf.votedFor, rf.lastIncludedIndex, rf.lastIncludedTerm,
            )
        } else {
            DbgPrintf(dRVRPC,
                "[%d] SVR %d <-RVRPC- SVR[%d] REJECT: votedfor[%d] lastLog|idx: %d, term: %d|\n",
                rf.currentTerm, rf.me, args.CandidateId,
                rf.votedFor, len(rf.log) - 1, rf.log[len(rf.log)-1].Term,
            )
        }
        if persistentStateModified { rf.persist() }
    }

    // just in case a leader fallback and reject RVRPC, in this case we need to rise election ticker
    if tickerShouldReset {
        rf.heartbeated = nil
        ech := make(chan bool)
        go rf.electionTicker(ech)

        finish, ok := <- ech
        if !ok { log.Fatalf("Error: SERVER[%d] CHANNEL crashed\n", rf.me) }
        if !finish {
            DbgPrintf(
                dWarn,
                "[%d] SVR %d  election ticker did not finish\n", rf.currentTerm, rf.me)
        }
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
func (rf *Raft) SendAndHandleRequestVoteRPC(idx int) {
    rf.mu.Lock()
    termBeforeSend := rf.currentTerm
    args := RequestVoteArgs{}
    args.Term           = rf.currentTerm
    args.CandidateId    = rf.me
    args.LastLogIndex   = rf.currentIndex
    if rf.currentIndex == rf.lastIncludedIndex {
        args.LastLogTerm = rf.lastIncludedTerm
    } else {
        args.LastLogTerm = rf.log[rf.currentIndex-rf.lastIncludedIndex-1].Term
    }
      
    rf.mu.Unlock()
    reply := RequestVoteReply{ -1, false }



    ok := rf.sendRequestVote(idx, &args, &reply)



    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.killed() { return }
    // if failed to send or ger result just return
    if !ok {
        DbgPrintf(
            dWarn, "[%d] SVR %d can not call RV to SVR[%d]\n",
            rf.currentTerm, rf.me, idx,
        )
        return
    }
    DbgPrintf(
        dRVRPC,
        "[%d] SVR %d -RVRPC-> SVR[%d]: result get [T:%d, G:%t]\n",
        rf.currentTerm, rf.me, idx, reply.Term, reply.VoteGranted,
    )
    // Get result (granted or not) check, check term 1st
    termBeforeUpdate := rf.currentTerm
    if rf.currentTerm < reply.Term {
        DbgPrintf(
            dRole,
            "[%d] SVR %d receive a advanced RVRPC result from[%d], step back to FOLLOWER with new term[%d]",
            rf.currentTerm, rf.me, idx, reply.Term)
        rf.currentTerm = reply.Term
        rf.role = FOLLOWER
        rf.voteCount = 0
        rf.votedFor = -1
        rf.persist()
    }
    // check if it is a old term rpc
    if termBeforeUpdate > termBeforeSend {
        DbgPrintf(
            dInfo, "[%d] SVR %d -RVRPC-> SVR[%d]: sendTerm:%d, resultTerm:%d RPC result get: old term, drop it\n",
            rf.currentTerm, rf.me, idx, termBeforeSend, termBeforeUpdate,
        )
        return
    }
    // just in case
    if rf.role == FOLLOWER {
        DbgPrintf(
            dInfo, "[%d] FLW %d -RVRPC-> SVR[%d]: result get, already step down to FLW, drop\n",
            rf.currentTerm, rf.me, idx,
        )
        return
    }

    // check the result:
    switch {
    case reply.VoteGranted:
        rf.voteCount += 1
        // got more votes than majority
        switch {
        case rf.role == LEADER:
            // already a leader, do nothing
            DbgPrintf(
                dInfo, "[%d] LDR %d -RVRPC-> SVR[%d]: already leader, drop it\n",
                rf.currentTerm, rf.me, idx,
            )
        case rf.voteCount >= rf.majority:
            // collect enough votes, become to leader
            // just in case 
            if rf.role == FOLLOWER {
                log.Fatalf("Error: A FOLLOWER[%d] wants to become a leader\n", rf.me)
            }
            rf.role = LEADER
            rf.nextIndex = make([]int, len(rf.peers))
            rf.matchIndex = make([]int, len(rf.peers))
            rf.assert(len(rf.log) + rf.lastIncludedIndex == rf.currentIndex, fmt.Sprintf(
                "len(log)=%d + LII(%d) != CI(%d)",
                len(rf.log), rf.lastIncludedIndex, rf.currentIndex,
            ))
            for i := range rf.peers {
                rf.nextIndex[i]     = rf.currentIndex + 1
                rf.matchIndex[i]    = 0 
            }
            DbgPrintf(dRole, "[%d] SVR %d become to LEADER\n", rf.currentTerm, rf.me)
            *rf.heartbeated = true
            // trigger heartbeat
            go rf.heartBeatThread()
        default:
            // not enough votes, wait and exit
            DbgPrintf(
                dInfo, "[%d] LDR %d <-RVRPC- SVR[%d]: not enough votes, wait\n",
                rf.currentTerm, rf.me, idx,
            )
        }
    case !reply.VoteGranted:
        if termBeforeSend < reply.Term {
            DbgPrintf(
                dRVRPC, "[%d] SVR %d -RVRPC-> SVR[%d]: REJECTED by term\n",
                rf.currentTerm, rf.me, idx,
            )
        } else {
            DbgPrintf(
                dRVRPC, "[%d] SVR %d -RVRPC-> SVR[%d]: REJECTED by stale log\n",
                rf.currentTerm, rf.me, idx,
            )
        }
    }
}

func (rf *Raft) electionTicker(c chan bool) {
    heartbeated := false
    hbIdx := rf.hbIdx
    rf.hbIdx += 1
    if rf.heartbeated != nil { log.Fatalln("Error: prev thread does not clear heartbeated flag") }
    rf.heartbeated = &heartbeated
    if heartbeated { log.Fatalln("Error: heartbeated before sleep in ticker") }
    ms := 500 + (rand.Int63() % 300)
    DbgPrintf(
        dTimer,
        "[%d] SVR %d state[%d] ticker start in round[%d] sleep %d milliseconds\n",
        rf.currentTerm, rf.me, rf.role, hbIdx, ms,
    )
    c <- true

    time.Sleep(time.Duration(ms) * time.Millisecond)
    rf.mu.Lock()
    if rf.killed() {
        rf.mu.Unlock()
        return
    }
    DbgPrintf(
        dTimer,
        "[%d] SVR %d state[%d] heartbeated?%t in round[%d]\n",
        rf.currentTerm, rf.me, rf.role, heartbeated, hbIdx,
    )
    if hbIdx < rf.hbIdx - 1 {
        rf.mu.Unlock()
        return
    }
    switch rf.role {
    case LEADER:
        // Leader does not need ticker
        rf.mu.Unlock()
        return
    case CANDIDATE:
        DbgPrintf(dTimer, "[%d] CAN %d rise re-election", rf.currentTerm, rf.me)
        rf.riseElection()
        return
    case FOLLOWER:
        if heartbeated {
            DbgPrintf(
                dTimer, "[%d] FLW %d heartbeated or votedFor %d",
                rf.currentTerm, rf.me, rf.votedFor)
            rf.mu.Unlock()
            return
        }
        DbgPrintf(dTimer, "[%d] FLW %d timeout rise election", rf.currentTerm, rf.me)
        rf.riseElection()
        return
    }
}
    
func (rf *Raft) riseElection() {
    ech := make(chan bool)
    DbgPrintf(dRole, "[%d] SVR %d become to CAN", rf.currentTerm, rf.me)
    rf.currentTerm += 1
    rf.role = CANDIDATE
    rf.voteCount = 1
    rf.votedFor = rf.me
    *rf.heartbeated = true
    rf.heartbeated = nil
    rf.persist()
    go rf.electionTicker(ech)
    finish, ok := <- ech
    if !ok { log.Fatalf("Error: SERVER[%d] CHANNEL crashed\n", rf.me) }
    if !finish { DbgPrintf(dWarn, "[%d] SVR %d election ticker did not finish\n",
        rf.currentTerm, rf.me) }
    rf.mu.Unlock()

    // send RVRPC to each server
    rf.mu.Lock()
    for idx := range rf.peers {
        if rf.killed() {
            DbgPrintf(dInfo, "[%d] SVR %d exit election: SERVER DOWN", rf.currentTerm, rf.me)
            break
        }
        if idx == rf.me { continue }
        if rf.role == FOLLOWER { break }
        rf.mu.Unlock()
        go rf.SendAndHandleRequestVoteRPC(idx)
        rf.mu.Lock()
    }
    rf.mu.Unlock()
}

func (rf *Raft) Sync() {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me { continue }
        rf.mu.Unlock()
        go rf.SendAndHandleAppendEntriesRPC(i, false)
        rf.mu.Lock()
    }
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
    rf.mu.Lock()
    defer rf.mu.Unlock()
    switch {
    case rf.role != LEADER:
        // do nothing b/c we are not a leader
        isLeader = false
    default:
        index, term, isLeader = rf.currentIndex + 1, rf.currentTerm, true
        rf.log = append(rf.log, Entry{ rf.currentTerm, command })
        rf.currentIndex += 1
        rf.persist()
        DbgPrintf(
            dInfo,
            "[%d] LDR %d received client reqeust, assign |idx:%d, value:%v|\n",
            rf.currentTerm, rf.me, len(rf.log) + rf.lastIncludedIndex, command,
        )
        // TODO: sync heartbeat should do all things for us?
        // in lab we can not b/c performance limit
        // go rf.Sync()
        go rf.Sync()
    }
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
    DbgPrintf(dInfo, "[%d] SVR %d killed", rf.currentTerm, rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
    rf.mu.Lock()
    heartbeated := false
    // startTerm := rf.currentTerm
    rf.heartbeated = &heartbeated
    
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
        switch {
        case rf.initTerm:
            DbgPrintf(dInfo,
                "[%d] SVR %d INIT\n", rf.currentTerm, rf.me,
            )
            // do nothing, unmark initial term flag
            rf.mu.Unlock()
        // case rf.currentTerm != startTerm && heartbeated:
        //    return
        case rf.role == FOLLOWER:
            if heartbeated {
                // some one trigger heartbeat thread just return
                rf.mu.Unlock()
                return
            } else {
                // time to rise election
                rf.riseElection()
                return
            }
        case rf.role == CANDIDATE:
            // no CANDIDATE should be here
            if heartbeated {
                rf.mu.Unlock()
                return
            } else {
                rf.mu.Unlock()
                log.Fatalf("Error: A CANDIDATE[%d] appears after 1st round\n", rf.me)
            }
        case rf.role == LEADER:
            // a sever should not become to a LEADER after the 1st round, panic
            if heartbeated {
                rf.mu.Unlock()
                return
            } else {
                rf.mu.Unlock()
                log.Fatalf("Error: A LEADER[%d] appears after 1st round\n", rf.me)
            }
        }

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

        rf.mu.Lock()
        rf.initTerm = false
	}
    rf.mu.Unlock()
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
    rf.initTerm = true

    rf.applyCh = applyCh

    rf.currentTerm  = 0
    rf.votedFor     = -1
    rf.log = make([]Entry, 0)
    // padding := "PADDING"
    // rf.log = append(rf.log, Entry{ 0, padding})
    rf.lastIncludedIndex = 0
    rf.lastIncludedTerm = -1
    rf.snapShot = nil


    rf.majority     = len(rf.peers) / 2 + 1
    rf.voteCount    = 0
    rf.role         = FOLLOWER
    // a ticker theard should register its own heartbeat flag
    // an heartbeat rpc should set old flag as true and re-register flag
    rf.heartbeated  = nil
    rf.hbIdx        = 0

    // Volatile state on all servers
    rf.commitIndex  = 0
    rf.lastApplied  = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
    
    // restore snapshot
    if rf.lastIncludedIndex > 0 {
        snapshot := persister.ReadSnapshot()
        rf.snapShot = &snapshot
    }

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
