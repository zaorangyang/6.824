package shardmaster

import (
	"github.com/Drewryz/6.824/labgob"
	"github.com/Drewryz/6.824/labrpc"
	"github.com/Drewryz/6.824/raft"
	"os"
	"sync"
	"time"
)

const opTimeOut = 2 * time.Second

type ShardMaster struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	finishChans  map[int]chan QueryReply
	reqTerm      map[int]int // 记录接收每个请求时的term
	persister    *raft.Persister
	maxraftstate int
	clerkBolts   map[int64]int64
	configs      []Config // indexed by config num
}

type Op struct {
	ClerkID   int64
	OpID      int64
	Op        string
	JoinArgs  JoinArgs
	LeaveArgs LeaveArgs
	MoveArgs  MoveArgs
	QueryArgs QueryArgs
}

func (sm *ShardMaster) getConfig(index int) Config {
	if index == -1 || index >= len(sm.configs) {
		index = len(sm.configs) - 1
	}
	config := sm.configs[index]
	shardsCopy := [NShards]int{}
	for i, v := range config.Shards {
		shardsCopy[i] = v
	}
	configCopy := Config{
		Num:    config.Num,
		Shards: shardsCopy,
		Groups: copyGroups(config.Groups),
	}
	return configCopy
}

func (sm *ShardMaster) opHandler(op Op) QueryReply {
	reply := QueryReply{}
	sm.mu.Lock()
	opIndex, term, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return reply
	}
	finishChan := make(chan QueryReply, 1)
	sm.finishChans[opIndex] = finishChan
	sm.reqTerm[opIndex] = term
	sm.mu.Unlock()

	timer := time.NewTimer(opTimeOut)
	select {
	case <-timer.C:
		reply.Err = "timeout"
	case value := <-finishChan:
		reply.WrongLeader = value.WrongLeader
		reply.Err = value.Err
		reply.Config = value.Config
	}

	sm.mu.Lock()
	delete(sm.finishChans, opIndex)
	delete(sm.reqTerm, opIndex)
	sm.mu.Unlock()
	return reply
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	DPrintf("[%v] get Join op, args:%v", sm.me, args)
	op := Op{
		Op:       "Join",
		JoinArgs: *args,
		ClerkID:  args.ClerkID,
		OpID:     args.OpID,
	}
	res := sm.opHandler(op)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
	if !reply.WrongLeader && len(reply.Err) == 0 {
		sm.mu.Lock()
		config := sm.getConfig(-1)
		sm.mu.Unlock()
		DPrintf("[%v] Join success, args:%v, config:%v", sm.me, args, config)
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		Op:        "Leave",
		LeaveArgs: *args,
		ClerkID:   args.ClerkID,
		OpID:      args.OpID,
	}
	res := sm.opHandler(op)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		Op:       "Move",
		MoveArgs: *args,
		ClerkID:  args.ClerkID,
		OpID:     args.OpID,
	}
	res := sm.opHandler(op)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{
		Op:        "Query",
		QueryArgs: *args,
		ClerkID:   args.ClerkID,
		OpID:      args.OpID,
	}
	res := sm.opHandler(op)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
	reply.Config = res.Config
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) doCommand(op Op) QueryReply {
	value := QueryReply{}
	switch op.Op {
	case "Join":
		joinArgs := op.JoinArgs
		lastConfig := sm.getConfig(-1)
		newConfig := Config{
			Num:    lastConfig.Num + 1,
			Groups: addGroups(lastConfig.Groups, joinArgs.Servers),
		}
		newConfig.Shards = getShards(lastConfig, len(newConfig.Groups), []int{}, joinArgs.Servers)
		sm.configs = append(sm.configs, newConfig)
	case "Leave":
		leaveArgs := op.LeaveArgs
		lastConfig := sm.getConfig(-1)
		newConfig := Config{
			Num:    lastConfig.Num + 1,
			Groups: removeGroups(lastConfig.Groups, leaveArgs.GIDs),
		}
		newConfig.Shards = getShards(lastConfig, len(newConfig.Groups), leaveArgs.GIDs, nil)
		sm.configs = append(sm.configs, newConfig)
	case "Move":
		moveArgs := op.MoveArgs
		lastConfig := sm.getConfig(-1)
		lastConfig.Shards[moveArgs.Shard] = moveArgs.GID
		newConfig := Config{
			Num:    lastConfig.Num + 1,
			Shards: lastConfig.Shards,
			Groups: lastConfig.Groups,
		}
		sm.configs = append(sm.configs, newConfig)

	case "Query":
		queryArgs := op.QueryArgs
		value.Config = sm.getConfig(queryArgs.Num)
	}
	return value
}

func getUnAssigedSahrds(curTopo map[int][]int, leaveGids []int) []int {
	// 遍历拓扑，拿到本来就没有分配的shard
	assignFlag := [NShards]int{}
	for _, gitShards := range curTopo {
		for _, shard := range gitShards {
			assignFlag[shard] = 1
		}
	}
	unAssigedSahrds := make([]int, 0)
	for shard, flag := range assignFlag {
		if flag == 0 {
			unAssigedSahrds = append(unAssigedSahrds, shard)
		}
	}
	// 下线节点承载的shard
	if len(leaveGids) > 0 {
		for _, leaveGid := range leaveGids {
			unAssigedSahrds = append(unAssigedSahrds, curTopo[leaveGid]...)
			delete(curTopo, leaveGid)
		}
	}
	return unAssigedSahrds
}

func getShards(lastConfig Config, newGroupNum int, leaveGids []int, addServers map[int][]string) [NShards]int {
	newShards := [NShards]int{}
	if newGroupNum == 0 {
		return newShards
	}
	// curTopo: gid -> shards[]
	curTopo := getCurTopo(lastConfig.Shards)
	unAssigedSahrds := getUnAssigedSahrds(curTopo, leaveGids)
	if len(addServers) > 0 {
		for gid, _ := range addServers {
			_, exist := curTopo[gid]
			if !exist {
				curTopo[gid] = make([]int, 0)
			}
		}
	}
	shardLoad := getShardLoad(newGroupNum)
	targetTopo := make(map[int][]int)
	left := 0
	right := len(shardLoad) - 1
	for gid, gidShards := range curTopo {
		if left > right || left >= len(shardLoad) || right < 0 {
			break
		}
		if len(gidShards) < shardLoad[right] {
			continue
		}
		if len(gidShards) > shardLoad[right] {
			unAssigedSahrds = append(unAssigedSahrds, gidShards[shardLoad[left]:]...)
			targetTopo[gid] = gidShards[:shardLoad[left]]
			left++
		} else {
			targetTopo[gid] = gidShards[:shardLoad[right]]
			right--
		}
	}
	for gid, gidShards := range curTopo {
		if left > right || left >= len(shardLoad) || right < 0 {
			break
		}
		if len(gidShards) >= shardLoad[right] {
			continue
		}
		for len(gidShards) < shardLoad[right] {
			gidShards = append(gidShards, unAssigedSahrds[len(unAssigedSahrds)-1])
			targetTopo[gid] = gidShards
			unAssigedSahrds = unAssigedSahrds[:len(unAssigedSahrds)-1]
		}
		right--
	}

	for gid, shards := range targetTopo {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	return newShards
}

// eg:对于shard为10，复制集个数为4的情况，得到的shardLoad为:[3,3,2,2]
func getShardLoad(groupNum int) []int {
	baseNum := NShards / groupNum
	remainNum := NShards % groupNum
	targetTopo := make([]int, groupNum)
	for i := 0; i < len(targetTopo); i++ {
		targetTopo[i] = baseNum
	}
	for i := 0; i < remainNum; i++ {
		targetTopo[i]++
	}
	return targetTopo
}

func getCurTopo(shards [NShards]int) map[int][]int {
	curTopo := make(map[int][]int)
	for i := 0; i < len(shards); i++ {
		_, exist := curTopo[shards[i]]
		if !exist {
			curTopo[shards[i]] = make([]int, 0)
		}
		curTopo[shards[i]] = append(curTopo[shards[i]], i)
	}
	delete(curTopo, 0)
	return curTopo
}

func copyGroups(src map[int][]string) map[int][]string {
	dest := make(map[int][]string)
	for gid, servers := range src {
		serversCopy := make([]string, len(servers))
		for i := 0; i < len(servers); i++ {
			serversCopy[i] = servers[i]
		}
		dest[gid] = serversCopy
	}
	return dest
}

// TODO: 未考虑为单个复制集删除服务器
func removeGroups(src map[int][]string, gids []int) map[int][]string {
	for _, gid := range gids {
		delete(src, gid)
	}
	return src
}

// TODO: 未考虑为单个复制集添加服务器
func addGroups(src map[int][]string, new map[int][]string) map[int][]string {
	for gid, servers := range new {
		src[gid] = servers
	}
	return src
}

func (sm *ShardMaster) apply() {
	for {
		select {
		case msg := <-sm.applyCh:
			if !msg.CommandValid {
				DPrintf("[server] CommandInvalid ")
				continue
			}

			op, ok := msg.Command.(Op)
			if !ok {
				DPrintf("server get unrecognized msg format")
				os.Exit(1)
			}

			sm.mu.Lock()
			var value QueryReply
			// 对于已经执行过的操作：
			// 1. bolt值不再变化
			// 2. 如果是Get方法且当前节点是leader，则返回数据
			// 3. 如果是Put/Append方法，则不执行
			bolt, ok := sm.clerkBolts[op.ClerkID]
			if ok && op.OpID < bolt {
				DPrintf("[server] get old op: %v", op)
				if op.Op == "Query" {
					value = sm.doCommand(op)
				}
			} else {
				value = sm.doCommand(op)
				sm.clerkBolts[op.ClerkID] = op.OpID + 1
			}
			finishChan, chanExist := sm.finishChans[msg.CommandIndex]
			term, termExist := sm.reqTerm[msg.CommandIndex]
			if chanExist && termExist && term == int(msg.Term) {
				finishChan <- value
			}
			sm.mu.Unlock()
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.finishChans = make(map[int]chan QueryReply)
	sm.reqTerm = make(map[int]int)
	sm.clerkBolts = make(map[int64]int64)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	go sm.apply()
	return sm
}
