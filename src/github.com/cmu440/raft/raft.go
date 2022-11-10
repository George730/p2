//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = NewPeer(...)
//   Create a new Raft server.
//
// rf.PutCommand(command interface{}) (index, term, isleader)
//   PutCommand agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me", its current term, and whether it thinks it
//   is a leader
//
// ApplyCommand
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyCommand to the service (e.g. tester) on the
//   same server, via the applyCh channel passed to NewPeer()
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/cmu440/rpc"
)

// Set to false to disable debug logs completely
// Make sure to set kEnableDebugLogs to false before submitting
const kEnableDebugLogs = true

// Set to true to log to stdout instead of file
const kLogToStdout = true

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

//
// ApplyCommand
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyCommand to the service (or
// tester) on the same server, via the applyCh passed to NewPeer()
//
type ApplyCommand struct {
	Index   int
	Command interface{}
}

const Follower = 1
const Candidate = 2
const Leader = 3

type logEntry struct {
	term      int
	logs      []int
	lastIndex int
	command   interface{}
}

//
// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
//
type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]
	// You are expected to create reasonably clear log files before asking a
	// debugging question on Piazza or OH. Use of this logger is optional, and
	// you are free to remove it completely.
	logger *log.Logger // We provide you with a separate logger per peer.

	// Your data here (2A, 2B).
	// Look at the Raft paper's Figure 2 for a description of what
	// state a Raft peer should maintain
	term        int
	status      int
	votedFor    int
	logs        map[int]logEntry
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine
	nextIndex   []int
	matchIndex  []int

	applyCh       chan ApplyCommand
	electTimeOut  chan bool
	appendEntries chan *AppendEntriesArgs
	beVoted       chan bool
}

//
// GetState()
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
//
func (rf *Raft) GetState() (int, int, bool) {
	rf.mux.Lock()
	defer rf.mux.Unlock()
	var me int
	var term int
	var isleader bool

	me = rf.me
	term = rf.term
	if rf.status == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return me, term, isleader
}

//
// RequestVoteArgs
// ===============
//
// Example RequestVote RPC arguments structure
//
// Please note
// ===========
// Field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term        int // candidate’s term
	CandidateId int // candidate requesting vote
	// LastLogIndex int // index of candidate’s last log entry
	// LastLogTerm  int // term of candidate’s last log entry

}

//
// RequestVoteReply
// ================
//
// Example RequestVote RPC reply structure.
//
// Please note
// ===========
// Field names must start with capital letters!
//
//
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// RequestVote
// ===========
//
// Example RequestVote RPC handler
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mux.Lock()

	if args.Term < rf.term {
		reply.VoteGranted = false
	} else {
		if args.Term > rf.term {
			rf.term = args.Term
		}

		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		} else {
			reply.VoteGranted = false
		}
	}

	reply.Term = rf.term
	rf.mux.Unlock()
}

//
// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a server
//
// server int -- index of the target server in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while
//
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the server side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	return ok
}

// AppendEntries RPC Begins
type AppendEntriesArgs struct {
	Term     int // leader’s term
	LeaderId int // so follower can redirect clients
	// PrevLogIndex int         // index of log entry immediately preceding new ones
	// PrevLogTerm  int         // term of prevLogIndex entry
	Entries int // log entries to store (empty for heartbeat; may send more than one for efficiency)
	// LeaderCommit int         // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for candidate to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mux.Lock()
	defer rf.mux.Unlock()

	// heartbeat
	if args.Entries == 0 {
		reply.Term = rf.term
		reply.Success = true
		rf.appendEntries <- args
		return
	}

	// rf.mux.Lock()
	// defer rf.mux.Unlock()

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Success = false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	return ok
}

// AppendEntries RPC Ends

func (rf *Raft) sendRequestVoteRoutine(
	server int, args *RequestVoteArgs, reply *RequestVoteReply, replies chan *RequestVoteReply) {
	rf.sendRequestVote(server, args, reply)
	replies <- reply // send reply through channel
}

func (rf *Raft) electionRoutine() {
	rf.mux.Lock()
	rf.logger.Println("Enter election")
	args := &RequestVoteArgs{
		rf.term,
		rf.me,
	}
	rf.mux.Unlock()

	// send initial empty AppendEntries RPCs (heartbeat) to each server;
	// repeat during idle periods to prevent election timeouts
	count := 1
	replies := make(chan *RequestVoteReply, len(rf.peers))
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer != rf.me {
			var reply RequestVoteReply
			go rf.sendRequestVoteRoutine(peer, args, &reply, replies)
		}
	}

	majority := len(rf.peers) / 2
	for {
		select {
		case <-rf.electTimeOut: // election timeout, just stop this routine
			// rf.logger.Println("Gather vote done but timeout")

			return
		case reply := <-replies: // gather replies from other servers
			rf.mux.Lock()
			currentTerm := rf.term
			rf.mux.Unlock()

			if reply.Term > currentTerm { // term mismatch, convert to follower and return
				rf.mux.Lock()
				rf.logger.Println("Term", rf.term, "Status:", rf.status, "Convert to follower")
				rf.status = Follower
				rf.votedFor = -1
				rf.term = reply.Term
				rf.mux.Unlock()
				return
			}

			// update vote counts
			if reply.VoteGranted {
				count += 1
			}
			rf.logger.Println("Enter replies, count:", count, "majority:", majority)

			// if satisfied majority vote, convert to leader and return
			if count > majority {
				rf.mux.Lock()
				rf.logger.Println("Term", rf.term, "Status:", rf.status, "Get majority")
				rf.beVoted <- true
				rf.mux.Unlock()
				return
			}
		}
	}
}

func (rf *Raft) leaderRoutine() {
	rf.mux.Lock()
	args := &AppendEntriesArgs{
		rf.term,
		rf.me,
		0,
	}
	rf.mux.Unlock()

	for {
		select {
		case <-rf.appendEntries:
			rf.mux.Lock()
			// rf.votedFor = -1
			rf.logger.Println("Term", rf.term, "Status:", rf.status, "Id", rf.me, "Response heartbeat")
			// if (rf.status == Leader || rf.status == Candidate) && appendEntry.Term >= rf.term {
			// 	// AppendEntries RPC received from new leader: convert to follower
			// 	rf.logger.Println("Term", rf.term, "Status:", rf.status, "Convert to follower")
			// 	rf.status = Follower
			// 	rf.electTimeOut <- true
			// }

			// if appendEntry.Term > rf.term {
			// 	rf.term = appendEntry.Term
			// }
			rf.mux.Unlock()
			return
		default:
			rf.logger.Println("Term", rf.term, "Status:", rf.status, "Id", rf.me, "Send heartbeat")
			for peer := 0; peer < len(rf.peers); peer++ {
				if peer != rf.me {
					var reply AppendEntriesReply
					go rf.sendAppendEntries(peer, args, &reply)
				}
			}

			time.Sleep(100 * time.Millisecond)
		}
	}
}

//
// PutCommand
// =====
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this server is not the leader, return false
//
// Otherwise start the agreement and return immediately
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term
//
// The third return value is true if this server believes it is
// the leader
//
func (rf *Raft) PutCommand(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B)

	return index, term, isLeader
}

//
// Stop
// ====
//
// The tester calls Stop() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Stop(), but it might be convenient to (for example)
// turn off debug output from this instance
//
func (rf *Raft) Stop() {
	// Your code here, if desired
	rf.logger.SetOutput(ioutil.Discard)
}

//
// NewPeer
// ====
//
// The service or tester wants to create a Raft server
//
// The port numbers of all the Raft servers (including this one)
// are in peers[]
//
// This server's port is peers[me]
//
// All the servers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyCommand messages
//
// NewPeer() must return quickly, so it should start Goroutines
// for any long-running work
//
func NewPeer(peers []*rpc.ClientEnd, me int, applyCh chan ApplyCommand) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	if kEnableDebugLogs {
		peerName := peers[me].String()
		logPrefix := fmt.Sprintf("%s ", peerName)
		if kLogToStdout {
			rf.logger = log.New(os.Stdout, peerName, log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt", kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			rf.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		rf.logger.Println("logger initialized")
	} else {
		rf.logger = log.New(ioutil.Discard, "", 0)
	}

	rf.term = 0
	rf.status = Follower
	rf.votedFor = -1
	rf.logs = make(map[int]logEntry)
	// rf.commitIndex = 0
	// rf.lastApplied = 0
	// rf.nextIndex = make([]int, len(peers))
	// rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh
	rf.electTimeOut = make(chan bool)
	rf.appendEntries = make(chan *AppendEntriesArgs)
	rf.beVoted = make(chan bool)

	go rf.mainRoutine()

	return rf
}

func (rf *Raft) mainRoutine() {
	for {
		randTime := rand.Intn(300) + 500
		electTimeOut := time.NewTimer(time.Duration(randTime) * time.Millisecond)
		rf.mux.Lock()
		rf.logger.Println("Term:", rf.term, "Status:", rf.status, "Id", rf.me, "Enter MianRoutine")
		rf.mux.Unlock()

		select {
		case <-electTimeOut.C: // The current election timeout
			electTimeOut.Stop()

			rf.mux.Lock()
			rf.logger.Println("Term:", rf.term, "Status:", rf.status, "Id", rf.me, "Current election timeout")
			rf.logger.Println("Votedfor:", rf.votedFor)
			if rf.votedFor != -1 {
				continue
			}
			if rf.status == Candidate {
				// election timeout elapses: start new election
				rf.logger.Println("Term", rf.term, "Status:", rf.status, "Id", rf.me, "Start new election")
				rf.electTimeOut <- true
			}
			rf.status = Candidate
			rf.term += 1
			rf.mux.Unlock()
			go rf.electionRoutine()
		case appendEntry := <-rf.appendEntries: // heartbeat
			electTimeOut.Stop()

			rf.mux.Lock()
			rf.votedFor = -1
			rf.logger.Println("Term", rf.term, "Status:", rf.status, "Id", rf.me, "Response heartbeat")
			if (rf.status == Leader || rf.status == Candidate) && appendEntry.Term >= rf.term {
				// AppendEntries RPC received from new leader: convert to follower
				rf.logger.Println("Term", rf.term, "Status:", rf.status, "Convert to follower")
				rf.status = Follower
				rf.electTimeOut <- true
			}

			if appendEntry.Term > rf.term {
				rf.term = appendEntry.Term
			}
			rf.mux.Unlock()
		case <-rf.beVoted: // Convert to leader and call leader routine
			electTimeOut.Stop()
			rf.mux.Lock()
			rf.logger.Println("Enter leader routine")
			rf.status = Leader
			rf.mux.Unlock()
			rf.leaderRoutine()
		}
	}
}
