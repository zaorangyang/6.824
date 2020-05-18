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
	"fmt"
	"github.com/Drewryz/6.824/labgob"
	"github.com/Drewryz/6.824/labrpc"
	"math/rand"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

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

type LogEntry struct {
	Command interface{}
	Term    uint64
}

type RaftRole int
type RpcMsg int

const (
	raftLogCapacity      = 1024 * 8
	HeartBeatInterval    = 100 * time.Millisecond
	ElectionTimeoutUpper = 800 * time.Millisecond
	ElectionTimeoutLower = 500 * time.Millisecond

	// 节点角色
	Follower  RaftRole = 0
	Candidate RaftRole = 1
	Leader    RaftRole = 2

	// appendrpc失败原因
	TermOlder       = 1
	LogUnconsistent = 2
	OlderRequst     = 3

	// raft节点状态
	Running = 0
	Stopped = 1
)

func getRandomDuration(rand *rand.Rand, lower time.Duration, upper time.Duration) time.Duration {
	durationRange := int(upper) - int(lower)
	return lower + time.Duration(rand.Intn(durationRange+1))
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	applyCh       chan ApplyMsg
	commitAlterCh chan struct{}
	// receiveRpcCh通道用于维持follower的状态
	receiveRpcCh chan struct{}
	rand         *rand.Rand

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        RaftRole
	currentTerm uint64
	// -1表示没有投票
	votedFor    int
	log         *RaftLog
	commitIndex uint64
	lastApplied uint64

	// leader
	nextIndex  []uint64
	matchIndex []uint64

	nodeState int32
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CurrentTerm  uint64
	CandidateId  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	CurrentTerm  uint64
	LeaderId     int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []*LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	FailReason    int
	ConflictTerm  uint64
	ConflictIndex uint64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = int(rf.currentTerm)
	if rf.role == Leader {
		isLeader = true
	}
	return term, isLeader
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
	err := e.Encode(rf.currentTerm)
	if err != nil {
		panic(err)
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		panic(err)
	}
	err = e.Encode(rf.log)
	if err != nil {
		panic(err)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.log = newRaftLog(raftLogCapacity)
		return
	}

	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	err := d.Decode(&rf.currentTerm)
	if err != nil {
		panic(err)
	}
	err = d.Decode(&rf.votedFor)
	if err != nil {
		panic(err)
	}
	err = d.Decode(&rf.log)
	if err != nil {
		panic(err)
	}
}

func logUptodate(firstTerm uint64, firstIndex uint64, secondTerm uint64, secondIndex uint64) bool {
	if firstTerm > secondTerm {
		return true
	}
	if firstTerm < secondTerm {
		return false
	}
	if firstIndex >= secondIndex {
		return true
	}
	return false
}

func getMajorityMatchIndex(matchIndex []uint64) uint64 {
	major := len(matchIndex)/2 + 1
	match := uint64(0)
	for i := 0; i < len(matchIndex); i++ {
		count := 0
		for j := 0; j < len(matchIndex); j++ {
			if matchIndex[i] <= matchIndex[j] {
				count++
			}
		}
		if count >= major && matchIndex[i] > match {
			match = matchIndex[i]
		}
	}
	return match
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 当前节点的term更大，返回false
	if args.CurrentTerm < rf.currentTerm {
		return
	}

	// 遇到新term
	if args.CurrentTerm > rf.currentTerm {
		rf.currentTerm = args.CurrentTerm
		rf.votedFor = -1
		rf.persist()
		if rf.role != Follower {
			rf.role = Follower
		}
	}

	// 投票。If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	lastLogEntry, lastLogIndex := rf.log.getLastLogEntry()
	atLeastUptodate := logUptodate(args.LastLogTerm, args.LastLogIndex, lastLogEntry.Term, lastLogIndex)
	voteGranted := (rf.votedFor == args.CandidateId || rf.votedFor == -1) && atLeastUptodate
	if voteGranted {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
	}

	// 当前节点投票给其他节点时，当前节点维持follower状态
	if voteGranted && rf.role == Follower && reply.VoteGranted {
		DPrintf("节点%d，RequestVote 保持follower状态", rf.me)
		select {
		case rf.receiveRpcCh <- struct{}{}:
		default:
		}
	}
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

func (rf *Raft) sendAndSolveRequestVote(getMajorityVotesCh chan struct{}) {
	wg := &sync.WaitGroup{}
	voteCh := make(chan struct{}, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			rf.mu.Lock()
			lastLog, lastLogIndex := rf.log.getLastLogEntry()
			args := &RequestVoteArgs{
				CurrentTerm:  rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLog.Term,
			}
			DPrintf("节点%d，sendAndSolveRequestVote,  currentTerm = %d", rf.me, rf.currentTerm)
			originTerm := rf.currentTerm
			rf.mu.Unlock()

			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if originTerm != rf.currentTerm {
				return
			}
			if ok {
				if !reply.VoteGranted {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
						rf.role = Follower
					}
				} else {
					select {
					case voteCh <- struct{}{}:
					default:
						DPrintf("can not be here!")
						os.Exit(-1)
					}
				}
			}
		}(i)
	}

	done := make(chan struct{}, 1)
	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	var voteCount int32
	for {
		select {
		case <-done:
			return
		case <-voteCh:
			voteCount++
			if voteCount >= int32(len(rf.peers)/2) {
				getMajorityVotesCh <- struct{}{}
				return
			}
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("节点%d接收到节点%d, AppendEntries before lock", rf.me, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("节点%d接收到节点%d, AppendEntries", rf.me, args.LeaderId)

	reply.Success = false
	reply.Term = rf.currentTerm

	// TODO: 此处if逻辑混乱，需要更改
	// 这个rpc调用，只要返回false，一定有相应的false原因

	// 当前节点term更新，返回false
	if args.CurrentTerm < rf.currentTerm {
		reply.FailReason = TermOlder
		return
	}
	// 收到当前leader的append rpc请求，当前节点保持follower状态
	if rf.role == Follower {
		DPrintf("节点%d，AppendEntries 保持follower状态", rf.me)
		select {
		case rf.receiveRpcCh <- struct{}{}:
		default:
		}
	}

	if args.CurrentTerm > rf.currentTerm {
		rf.currentTerm = args.CurrentTerm
		rf.votedFor = -1
		rf.persist()
		if rf.role != Follower {
			rf.role = Follower
		}
	}
	// candidate节点发现了当前时段的leader, 转换为follower
	if args.CurrentTerm == rf.currentTerm && rf.role == Candidate {
		rf.role = Follower
	}

	entry := rf.log.getLogEntryByIndex(args.PrevLogIndex)
	// 日志不一致返回false
	if entry == nil || entry.Term != args.PrevLogTerm {
		reply.FailReason = LogUnconsistent
		if entry == nil {
			reply.ConflictIndex = rf.log.getLastLogEntryIndex() + 1
			reply.ConflictTerm = 0
		} else {
			reply.ConflictTerm = rf.log.getLogEntryByIndex(args.PrevLogIndex).Term
			reply.ConflictIndex = rf.getTermFisrtlogIndex(reply.ConflictTerm, args.PrevLogIndex)
		}
		return
	}

	// 如果要复制的日志与当前节点的日志不一致
	if !rf.log.compareEntries(args.PrevLogIndex, args.Entries) {
		if rf.commitIndex > args.PrevLogIndex {
			reply.FailReason = OlderRequst
			return
		}
		rf.log.deleteEntriesByIndex(args.PrevLogIndex)
		rf.log.appendEntries(args.Entries)
		rf.persist()
	}
	reply.Success = true

	// 此时，leader的日志与当前节点到args.PrevLogIndex+len(args.Entries)为止的日志一致
	if args.LeaderCommit > rf.commitIndex {
		// follower在调整commitIndex时，要确保自身的日志与leader日志一致
		rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex+uint64(len(args.Entries)))
		select {
		case rf.commitAlterCh <- struct{}{}:
		default:
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("节点%d向节点%d, sendAppendEntries", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		DPrintf("节点%d向节点%d, sendAppendEntries, 成功", rf.me, server)
	} else {
		DPrintf("节点%d向节点%d, sendAppendEntries, 失败", rf.me, server)
	}
	return ok
}

// TODO: 转换成follower应该如何优雅地做
func (rf *Raft) sendAndSolveAppendEntries(server int, args *AppendEntriesArgs, isHeartbeat bool) {
	defer func() {
		if r := recover(); r != nil {
			DPrintf("%v", r)
			debug.PrintStack()
			DPrintf("[%d] rf.nextIndex[server]= %d || log: %s || rf.role=%d", rf.me, rf.nextIndex[server], rf.log.getLogStr(), rf.role)
			os.Exit(1)
		}
	}()

	for {
		// TODO: 完善raft优雅退出机制. 测试组件下线一个raft节点时，只是将其从集群的网络中隔离，这导致被下线的节点重复创建用于heartbeat和
		//  复制日志的协程，从而导致TestFigure82C这个case协程超限：race: limit on 8128 simultaneously alive goroutines is exceeded, dying
		if atomic.LoadInt32(&rf.nodeState) == Stopped {
			return
		}
		rf.mu.Lock()
		// 当前leader可能已经转换为了follower，快速返回
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.log.getLogEntryByIndex(args.PrevLogIndex).Term
		if !isHeartbeat {
			args.Entries = rf.log.getLogEntryByRange(rf.nextIndex[server], rf.log.getLastLogEntryIndex()+1)
		}
		nextIndexBak := rf.nextIndex[server]
		reply := &AppendEntriesReply{}
		originTerm := rf.currentTerm
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(server, args, reply)
		// rpc调用失败
		if !ok {
			return
		}
		rf.mu.Lock()
		if originTerm != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		// 节点返回成功
		if reply.Success {
			// 由于遇到网络延迟，使返回的结果失去时效性，则不更改nextIndex和matchIndex
			if atomic.CompareAndSwapUint64(&rf.nextIndex[server], nextIndexBak, nextIndexBak+uint64(len(args.Entries))) {
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
			rf.mu.Unlock()
			return
		}
		// 有两种情况对方节点会返回失败：1. 对方节点的term更新 2. 声明的日志不一致。但是，当前节点的term可能会在其他地方被更新，
		// 所以，无法通过简单比较回复的term与当前节点的term对比，来判断到底是由于何种原因，对方节点返回失败了
		if reply.FailReason == TermOlder {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				rf.role = Follower
			}
			rf.mu.Unlock()
			return
		}
		// PreLogIndex位置的日志与leader不一致
		if reply.FailReason == LogUnconsistent {
			// 日志回溯优化
			var newNextIndex uint64
			if reply.ConflictTerm == 0 {
				newNextIndex = reply.ConflictIndex
			} else {
				searchRes := rf.getTermLastlogIndex(reply.ConflictTerm, args.PrevLogIndex)
				if searchRes == 0 {
					newNextIndex = reply.ConflictIndex
				} else {
					newNextIndex = searchRes + 1
				}
			}
			// nextIndex失去时效性，则直接退出
			if !atomic.CompareAndSwapUint64(&rf.nextIndex[server], nextIndexBak, newNextIndex) {
				rf.mu.Unlock()
				return
			}
		}

		if reply.FailReason == OlderRequst {
			rf.mu.Unlock()
			return
		}

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

	term, isLeader := rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = int(rf.log.appendEntries([]*LogEntry{
		&LogEntry{
			Command: command,
			Term:    uint64(term),
		},
	}))
	rf.persist()
	rf.nextIndex[rf.me]++
	rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	atomic.StoreInt32(&rf.nodeState, Stopped)
}

func (rf *Raft) getTermFisrtlogIndex(term uint64, guard uint64) uint64 {
	for rf.log.getLogEntryByIndex(guard).Term == term {
		guard--
	}
	return guard + 1
}

func (rf *Raft) getTermLastlogIndex(term uint64, guard uint64) uint64 {
	for guard > 0 && rf.log.getLogEntryByIndex(guard).Term != term {
		guard--
	}
	return guard
}

func (rf *Raft) doApplyMsg() {
	for {
		select {
		case <-rf.commitAlterCh:
			for {
				rf.mu.Lock()
				if rf.commitIndex <= rf.lastApplied {
					rf.mu.Unlock()
					break
				}
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log.getLogEntryByIndex(rf.lastApplied + 1).Command,
					CommandIndex: int(rf.lastApplied + 1),
				}
				rf.mu.Unlock()

				rf.applyCh <- applyMsg

				rf.mu.Lock()
				rf.lastApplied++
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) followerFlow() {
	for {
		// 当follower一段时间内：
		// 1. 没有投票给其他节点
		// 2. 没有收到当前leader的append rpc请求
		// 会转换为candidate.
		electionTimeout := getRandomDuration(rf.rand, ElectionTimeoutLower, ElectionTimeoutUpper)
		timer := time.NewTimer(electionTimeout)
		select {
		case <-timer.C:
			rf.mu.Lock()
			rf.role = Candidate
			rf.mu.Unlock()
			return
		case <-rf.receiveRpcCh:
		}
	}
}

func (rf *Raft) candidateFlow() {
	count := 0
	defer func() {
		DPrintf("节点%d，经过%d轮选举", rf.me, count)
	}()
	for {
		count++
		getMajorityVotesCh := make(chan struct{}, 1)
		rf.mu.Lock()
		if rf.role == Follower {
			rf.mu.Unlock()
			return
		}
		rf.currentTerm++
		DPrintf("节点%d，candidateFlow,  currentTerm = %d", rf.me, rf.currentTerm)
		rf.votedFor = rf.me
		rf.persist()
		rf.mu.Unlock()
		electionTimeout := getRandomDuration(rf.rand, ElectionTimeoutLower, ElectionTimeoutUpper)
		timer := time.NewTimer(electionTimeout)
		go rf.sendAndSolveRequestVote(getMajorityVotesCh)
		select {
		case <-timer.C:
		case <-getMajorityVotesCh:
			rf.mu.Lock()
			if rf.role == Follower {
				rf.mu.Unlock()
				return
			}
			rf.role = Leader
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) doReplicateLogOrHearbeart(isHeartbeat bool) {
	// 由于currentTerm可能在处理appendRPC的返回结果时发生改动，故需要记录
	currentTerm := rf.currentTerm
	// commitIndex可能会在leder接下来的流程发生改动，因此同样提前记录
	commitIndex := rf.commitIndex
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := &AppendEntriesArgs{
			CurrentTerm:  currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: commitIndex,
		}
		if isHeartbeat || rf.log.getLastLogEntryIndex() >= rf.nextIndex[i] {
			go rf.sendAndSolveAppendEntries(i, args, isHeartbeat)
		}
	}
}

func (rf *Raft) leaderFlow() {
	rf.mu.Lock()
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.log.getLastLogEntryIndex() + 1
	}
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			DPrintf("%v", r)
			debug.PrintStack()

			matchIndexStr := []string{}
			for _, m := range rf.matchIndex {
				matchIndexStr = append(matchIndexStr, string(m))
			}
			matchIndexStrJoin := strings.Join(matchIndexStr, ",")
			logStr := rf.log.getLogStr()
			DPrintf("[%d] matchIndex: %s || log: %s", rf.me, matchIndexStrJoin, logStr)

			os.Exit(1)
		}
	}()

	for {
		DPrintf("节点%d, leader main flow start", rf.me)
		rf.mu.Lock()
		if rf.role == Follower {
			rf.mu.Unlock()
			return
		}
		DPrintf("节点%d, leader main flow will send AppendEntries", rf.me)
		// 复制日志
		rf.doReplicateLogOrHearbeart(false)
		rf.mu.Unlock()

		timer := time.NewTimer(HeartBeatInterval)
		select {
		case <-timer.C:
			rf.mu.Lock()
			// heartbeat
			rf.doReplicateLogOrHearbeart(true)
			rf.mu.Unlock()
		}

		// 更新commit
		rf.mu.Lock()
		// 快速结束。原因：即使日志被复制到大多数节点，也有可能被新的leader删除，如果不快速结束，可能会发生空指针错误
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		match := getMajorityMatchIndex(rf.matchIndex)
		if rf.log.getLogEntryByIndex(match).Term == rf.currentTerm && match > rf.commitIndex {
			rf.commitIndex = match
			select {
			case rf.commitAlterCh <- struct{}{}:
			default:
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) run() {
	go rf.doApplyMsg()
	for {
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()

		switch role {
		case Follower:
			DPrintf("节点%d, become Follower", rf.me)
			rf.followerFlow()
		case Candidate:
			DPrintf("节点%d, become Candidate", rf.me)
			rf.candidateFlow()
		case Leader:
			DPrintf("节点%d, become Leader", rf.me)
			rf.leaderFlow()
		}
	}
}

func (rf *Raft) GetLogStr() string {
	return fmt.Sprintf("%d, %d, %s", rf.role, rf.currentTerm, rf.log.getLogStr())

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
	rf.commitAlterCh = make(chan struct{}, 1)
	rf.receiveRpcCh = make(chan struct{}, 1)
	rf.nextIndex = make([]uint64, len(rf.peers))
	rf.matchIndex = make([]uint64, len(rf.peers))
	rf.rand = rand.New(rand.NewSource(time.Now().UnixNano() - int64(rf.me)))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	atomic.StoreInt32(&rf.nodeState, Running)
	go rf.run()

	return rf
}

func getEntriesStr(entries []*LogEntry) string {
	entriesStr := []string{}
	for _, entry := range entries {
		entriesStr = append(entriesStr, fmt.Sprintf("%v", *entry))
	}
	return strings.Join(entriesStr, ",")
}
