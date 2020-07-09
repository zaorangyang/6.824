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

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Term         uint64
	Snapshot     Snapshot
}

type LogEntry struct {
	Command interface{}
	Term    uint64
}

type Snapshot struct {
	LastIndex uint64
	LastTerm  uint64
	State     map[string]string
	// TODO: raft作为一个通用库，不应该对server的业务逻辑有知晓
	ClerkBolts map[int64]int64
}

type RaftRole int
type RpcMsg int

const (
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

type SnapshotArgs struct {
	CurrentTerm uint64
	LeaderId    int
	Snapshot    Snapshot
}

type SnapshotReply struct {
	Term uint64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = int(rf.currentTerm)
	if rf.role == Leader {
		isLeader = true
	}
	return term, isLeader
}

func (rf *Raft) getRaftPersistentState() []byte {
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
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	data := rf.getRaftPersistentState()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.log = newRaftLog()
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

func (rf *Raft) InstallSnapshot(args *SnapshotArgs, reply *SnapshotReply) {
	DPrintf("InstallSnapshot called")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
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

	curSnapshot, ok := readSnapShot(rf.persister)
	if ok && curSnapshot.LastIndex >= args.Snapshot.LastIndex {
		DPrintf("InstallSnapshot receive old snapshot")
		return
	}

	rf.makeSnapshotWithoutLock(&args.Snapshot)
}

func (rf *Raft) sendInstallSnapshot(server int, args *SnapshotArgs, reply *SnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) makeInstallSnapshotArgs() SnapshotArgs {
	snapshot, ok := readSnapShot(rf.persister)
	if !ok {
		DPrintf("invalid snapshot in leader calling InstallSnapshot")
		os.Exit(3)
	}

	args := SnapshotArgs{
		CurrentTerm: rf.currentTerm,
		LeaderId:    rf.me,
		Snapshot:    snapshot,
	}
	return args
}

func (rf *Raft) makeSnapshotWithoutLock(snapshot *Snapshot) {
	rf.log.discardOldLog(snapshot.LastIndex)
	rf.log.setSnapshotLastIndexAndSnapshotLastTerm(snapshot.LastIndex, snapshot.LastTerm)
	raftData := rf.getRaftPersistentState()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(snapshot)
	if err != nil {
		panic(err)
	}
	snapshotData := w.Bytes()

	rf.persister.SaveStateAndSnapshot(raftData, snapshotData)
}

func (rf *Raft) MakeSnapshot(snapshot *Snapshot) {
	DPrintf("MakeSnapshot called")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.makeSnapshotWithoutLock(snapshot)
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
	lastLogIndex, lastLogTerm := rf.log.getLastLogEntryIndexAndTerm()
	atLeastUptodate := logUptodate(args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex)
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
			lastLogIndex, lastLogTerm := rf.log.getLastLogEntryIndexAndTerm()
			args := &RequestVoteArgs{
				CurrentTerm:  rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
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

	// TODO: 如果遇到很旧的包，args.PrevLogIndex指向的日志已经做了snapshot，会发生什么？
	// 这种情况直接返回OlderRequest
	// TODO: getLogEntryByIndex这个函数，不能返回snapshot中的lastindex，lastterm，这是个错误

	inSnapshot, entry := rf.log.getLogEntryByIndex(args.PrevLogIndex)
	if inSnapshot {
		reply.FailReason = OlderRequst
		return
	}
	// 日志不一致返回false
	if entry == nil || entry.Term != args.PrevLogTerm {
		reply.FailReason = LogUnconsistent
		// 日志回溯优化
		if entry == nil { // PrevLogIndex不存在日志
			reply.ConflictIndex = rf.log.getLastLogEntryIndex() + 1
			reply.ConflictTerm = 0
		} else { // PrevLogIndex存在日志，但是Term不一致
			reply.ConflictTerm = entry.Term
			reply.ConflictIndex = rf.getFisrtConflictIndexByConflictTerm(reply.ConflictTerm, args.PrevLogIndex)
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

func (rf *Raft) solveInstallSnapshot(server int, snapshot *Snapshot, reply *SnapshotReply, originTerm uint64, nextIndexBak uint64) {
	if originTerm != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		rf.role = Follower
		return
	}

	if atomic.CompareAndSwapUint64(&rf.nextIndex[server], nextIndexBak, snapshot.LastIndex+1) {
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}
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
		nextIndexBak := rf.nextIndex[server]
		originTerm := rf.currentTerm

		// TODO: snapshot的几个状态应该缓冲起来，不然实际生产环境中持续读快照会浪费太多的IO资源
		snapshot, ok := readSnapShot(rf.persister)
		if ok {
			if args.PrevLogIndex == snapshot.LastIndex {
				args.PrevLogTerm = snapshot.LastTerm
			}
			if args.PrevLogIndex < snapshot.LastIndex {
				snapshotArgs := rf.makeInstallSnapshotArgs()
				snapshotReply := SnapshotReply{}
				rf.mu.Unlock()
				ok = rf.sendInstallSnapshot(rf.me, &snapshotArgs, &snapshotReply)
				if !ok {
					return
				}
				rf.mu.Lock()
				rf.solveInstallSnapshot(server, &snapshot, &snapshotReply, originTerm, nextIndexBak)
				rf.mu.Unlock()
				return
			}
		} else {
			_, entry := rf.log.getLogEntryByIndex(args.PrevLogIndex)
			args.PrevLogTerm = entry.Term
		}
		if !isHeartbeat {
			args.Entries = rf.log.getLogEntryByRange(rf.nextIndex[server], rf.log.getLastLogEntryIndex()+1)
		}
		reply := &AppendEntriesReply{}
		rf.mu.Unlock()

		ok = rf.sendAppendEntries(server, args, reply)
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
				searchRes := rf.getLastlogIndexByTerm(reply.ConflictTerm, args.PrevLogIndex)
				if searchRes == 0 { // 未找到，跳过ConflictTerm
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

// 从guard开始向前搜索，找到第一条周期为conflictTerm的日志索引
func (rf *Raft) getFisrtConflictIndexByConflictTerm(conflictTerm uint64, guard uint64) uint64 {
	for {
		inSnapshot, entry := rf.log.getLogEntryByIndex(guard)
		if inSnapshot {
			// tracky here. 回溯到snapshot中时，ConflictIndex应该是snapshot.LastIndex-1+2
			return guard + 2
		}
		if entry.Term != conflictTerm {
			return guard + 1
		}
		guard--
	}
}

// 从guard开始向前搜索，找到最后一条周期为term的日志索引
func (rf *Raft) getLastlogIndexByTerm(term uint64, guard uint64) uint64 {
	for guard > 0 {
		inSnapshot, entry := rf.log.getLogEntryByIndex(guard)
		if inSnapshot { // 回溯到snapshot中表示未找到
			return 0
		}
		if entry.Term == term {
			return guard
		}
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
				var applyMsg ApplyMsg
				if rf.lastApplied+1 < rf.log.Base {
					snapshot, ok := readSnapShot(rf.persister)
					if !ok {
						panic("read invalid snapshot in raft doApplyMsg")
					}
					applyMsg = ApplyMsg{
						CommandValid: false,
						Snapshot:     snapshot,
					}
					rf.lastApplied = snapshot.LastIndex
				} else {
					_, entry := rf.log.getLogEntryByIndex(rf.lastApplied + 1)
					applyMsg = ApplyMsg{
						CommandValid: true,
						Command:      entry.Command,
						CommandIndex: int(rf.lastApplied + 1),
						Term:         entry.Term,
					}
					rf.lastApplied++
				}
				rf.mu.Unlock()
				rf.applyCh <- applyMsg
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
		if match > rf.commitIndex {
			// snapshot针对的是已经提交的日志，此时getLogEntryByIndex(match)得到的log一定是有效的
			_, entry := rf.log.getLogEntryByIndex(match)
			if entry.Term == rf.currentTerm {
				rf.commitIndex = match
				select {
				case rf.commitAlterCh <- struct{}{}:
				default:
				}
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
