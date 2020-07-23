package shardkv

import (
	"github.com/Drewryz/6.824/labgob"
	"github.com/Drewryz/6.824/labrpc"
	"github.com/Drewryz/6.824/raft"
	"github.com/Drewryz/6.824/shardmaster"
	"log"
	"sync"
	"time"
)

func init() {
	labgob.Register(shardmaster.Config{})
	labgob.Register(GetingDataArgs{})
	labgob.Register(GetingDataReply{})
	labgob.Register(map[string]string{})
	labgob.Register(Op{})
}

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const opTimeOut = 2 * time.Second
const pullConfigInterval = 80 * time.Millisecond
const pullDataInterval = 60 * time.Millisecond

type Op struct {
	ClerkID int64
	OpID    int64
	Op      string
	Key     string
	Value   string
}

type opReply struct {
	value string
	error string
}

type ShardKV struct {
	mu                 sync.Mutex
	me                 int
	rf                 *raft.Raft
	applyCh            chan raft.ApplyMsg
	finishChans        map[int]chan opReply
	reqTerm            map[int]int // 记录接收每个请求时的term
	data               map[string]string
	clerkBolts         map[int64]int64 // TODO: 客户端门限没有做过期处理
	persister          *raft.Persister
	make_end           func(string) *labrpc.ClientEnd
	gid                int
	masters            []*labrpc.ClientEnd
	maxraftstate       int // snapshot if log grows this big
	curConfig          shardmaster.Config
	ownShards          map[int]struct{}
	ownShardsConfigNum map[int]int                       // shard -> configNum
	pendingData        map[int]map[int]map[string]string // configNum -> shard -> kv data
	waitingShards      map[int]int                       // shard -> configNum
	pullConfigTimer    *time.Timer
	pullDataTimer      *time.Timer
	mck                *shardmaster.Clerk
}

func (kv *ShardKV) opHelper(op *Op) GetReply {
	DPrintf("[%v:%v] get op, %v", kv.gid, kv.me, *op)
	// tricy here: 用GetReply作为通用的reply
	reply := GetReply{}
	kv.mu.Lock()
	_, exist := kv.ownShards[key2shard(op.Key)]
	if !exist {
		DPrintf("[%v:%v] cannot find shard=%v, in ownshards=%v, curConfig=%v, waitingShards=%v", kv.gid, kv.me, key2shard(op.Key), kv.ownShards, kv.curConfig, kv.waitingShards)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return reply
	}
	opIndex, term, isLeader := kv.rf.Start(*op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return reply
	}
	finishChan := make(chan opReply, 1)
	kv.finishChans[opIndex] = finishChan
	kv.reqTerm[opIndex] = term
	kv.mu.Unlock()

	var opReply opReply
	timer := time.NewTimer(opTimeOut)
	select {
	case <-timer.C:
		reply.Err = "timeout"
	case opReply = <-finishChan:
		reply.Err = Err(opReply.error)
		reply.Value = opReply.value
	}

	kv.mu.Lock()
	delete(kv.finishChans, opIndex)
	delete(kv.reqTerm, opIndex)
	kv.mu.Unlock()

	return reply
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		ClerkID: args.ClerkID,
		OpID:    args.OpID,
		Key:     args.Key,
		Op:      "Get",
	}
	ret := kv.opHelper(&op)
	reply.WrongLeader = ret.WrongLeader
	reply.Value = ret.Value
	reply.Err = ret.Err
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		ClerkID: args.ClerkID,
		OpID:    args.OpID,
		Key:     args.Key,
		Op:      args.Op,
		Value:   args.Value,
	}
	ret := kv.opHelper(&op)
	reply.WrongLeader = ret.WrongLeader
	reply.Err = ret.Err
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) doCommand(op Op) (string, string) {
	var value string
	var ok bool
	var error string
	_, exist := kv.ownShards[key2shard(op.Key)]
	if !exist {
		return "", ErrWrongGroup
	}
	switch op.Op {
	case "Get":
		value, ok = kv.data[op.Key]
		if !ok {
			DPrintf("[server] dnot have key: %v", op.Key)
			value = ""
			error = ErrNoKey
		} else {
			error = OK
		}
	case "Put":
		kv.data[op.Key] = op.Value
		error = OK
	case "Append":
		value, ok := kv.data[op.Key]
		if !ok {
			kv.data[op.Key] = op.Value
		} else {
			kv.data[op.Key] = value + op.Value
		}
		error = OK
	}
	return value, error
}

// 处理用户请求，Get/Put/Append
func (kv *ShardKV) solveOp(op Op, commandIndex int, commandTerm uint64) {
	kv.mu.Lock()
	var value string
	var error string
	// 对于已经执行过的操作：
	// 1. bolt值不再变化
	// 2. 如果是Get方法且当前节点是leader，则返回数据
	// 3. 如果是Put/Append方法，则不执行
	bolt, ok := kv.clerkBolts[op.ClerkID]
	if ok && op.OpID < bolt {
		DPrintf("[server] get old op: %v", op)
		if op.Op == "Get" {
			value, error = kv.doCommand(op)
		}
	} else {
		value, error = kv.doCommand(op)
		kv.clerkBolts[op.ClerkID] = op.OpID + 1
	}
	finishChan, chanExist := kv.finishChans[commandIndex]
	term, termExist := kv.reqTerm[commandIndex]
	if chanExist && termExist && term == int(commandTerm) {
		finishChan <- opReply{value: value, error: error}
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) solveConfig(config shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if config.Num <= kv.curConfig.Num {
		return
	}
	newShards := map[int]struct{}{}
	for shard, gid := range config.Shards {
		if gid == kv.gid {
			newShards[shard] = struct{}{}
		}
	}
	if len(kv.ownShards) > 0 {
		kv.pendingData[kv.curConfig.Num] = make(map[int]map[string]string)
		for oldShard := range kv.ownShards {
			// 拥有的shard不在新的配置中，挂起
			_, exist := newShards[oldShard]
			if !exist {
				kv.pendingData[kv.curConfig.Num][oldShard] = make(map[string]string)
				for key, val := range kv.data {
					if key2shard(key) == oldShard {
						kv.pendingData[kv.curConfig.Num][oldShard][key] = val
						delete(kv.data, key)
					}
				}
				delete(kv.ownShards, oldShard)
				delete(kv.ownShardsConfigNum, oldShard)
			}
		}
	}
	for newShard := range newShards {
		// 复制集第一次被分配的shard直接拥有即可
		if kv.curConfig.Num == 0 {
			kv.ownShards[newShard] = struct{}{}
			kv.ownShardsConfigNum[newShard] = config.Num
			continue
		}
		// 新配置中有没有拥有的shard，waiting
		_, exist := kv.ownShards[newShard]
		_, waiting := kv.waitingShards[newShard]
		if !exist && !waiting {
			kv.waitingShards[newShard] = kv.curConfig.Num
		}
	}
	kv.curConfig = config
}

func (kv *ShardKV) solveGotData(reply GetingDataReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, exist := kv.waitingShards[reply.Shard]
	if !exist {
		return
	}
	configNum, exist := kv.ownShardsConfigNum[reply.Shard]
	if exist && reply.ConfigNum <= configNum {
		return
	}
	for key, value := range reply.Data {
		reply.Data[key] = value
	}
	kv.ownShards[reply.Shard] = struct{}{}
	kv.ownShardsConfigNum[reply.Shard] = reply.ConfigNum
	delete(kv.waitingShards, reply.Shard)
}

func (kv *ShardKV) apply() {
	for {
		select {
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				// TODO: 这里需要加锁，但是不加锁也没有出错, wtf
				DPrintf("[server] apply snapshot ")
				snapshot := msg.Snapshot
				kv.clerkBolts = snapshot.ClerkBolts
				kv.data = snapshot.State
				continue
			}
			switch v := msg.Command.(type) {
			case Op:
				kv.solveOp(v, msg.CommandIndex, msg.Term)
			case shardmaster.Config:
				kv.solveConfig(v)
			case GetingDataReply:
				kv.solveGotData(v)
			}
			kv.makeSnapshot(msg.Term, uint64(msg.CommandIndex))
		}
	}
}

func (kv *ShardKV) makeSnapshot(lastTerm uint64, lastIndex uint64) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() <= kv.maxraftstate {
		return
	}
	snapshot := raft.Snapshot{
		LastIndex:  lastIndex,
		LastTerm:   lastTerm,
		State:      kv.data,
		ClerkBolts: kv.clerkBolts,
	}
	kv.rf.MakeSnapshot(&snapshot)
}

// leader拉取下一个Config, 并通过raft同步
func (kv *ShardKV) pullConfig() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.pullConfigTimer.Reset(pullConfigInterval)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	if len(kv.waitingShards) > 0 {
		return
	}
	nextConfigNum := kv.curConfig.Num + 1
	nextConfig := kv.mck.Query(nextConfigNum)
	// TODO: 如果curConfig迟迟没有得到更新，会有大量的协程涌入
	if nextConfig.Num > kv.curConfig.Num {
		kv.rf.Start(nextConfig.Copy())
	}
}

type GetingDataArgs struct {
	Shard     int
	ConfigNum int
}

type GetingDataReply struct {
	Err       string
	Data      map[string]string
	Shard     int
	ConfigNum int
}

func (kv *ShardKV) GetingData(args *GetingDataArgs, reply *GetingDataReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.curConfig.Num < args.ConfigNum {
		reply.Err = ErrWrongGroup
		return
	}
	shardsData, exist := kv.pendingData[args.ConfigNum]
	if !exist {
		reply.Err = ErrWrongGroup
		return
	}
	data, exist := shardsData[args.Shard]
	if !exist {
		reply.Err = ErrWrongGroup
		return
	}
	reply.Shard = args.Shard
	reply.Data = data
	reply.ConfigNum = args.ConfigNum
}

// 从config中拉取单个shard数据
func (kv *ShardKV) pullShard(shard int, configNum int) {
	config := kv.mck.Query(configNum)
	if config.Num != configNum {
		return
	}
	servers := config.Groups[config.Shards[shard]]
	for _, server := range servers {
		args := GetingDataArgs{
			Shard:     shard,
			ConfigNum: configNum,
		}
		reply := GetingDataReply{}
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.GetingData", &args, &reply)
		if !ok || len(reply.Err) != 0 {
			continue
		}
		// TODO: 这里没有等待apply便返回，raft log中可能会存在大量的重复reply
		_, _, isLeader := kv.rf.Start(reply)
		if isLeader {
			break
		}
	}
}

// leader拉取所有waiting的shards。
// 这个函数返回后，主要有以下几种情况：
// 1. 所有waiting的数据都被成功安装，皆大欢喜
// 2. 部分成功，部分失败，等待下次拉取
// 3. 正在被raft同步，还未更新到状态机中
// 简而言之，这个函数返回与否，不能确定waiting shards是否已经成功安装，需要做去重操作
func (kv *ShardKV) pullData() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.pullDataTimer.Reset(pullDataInterval)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	if len(kv.waitingShards) == 0 {
		return
	}
	wg := &sync.WaitGroup{}
	for shard, configNum := range kv.waitingShards {
		wg.Add(1)
		go func(shard int, configNum int) {
			kv.pullShard(shard, configNum)
			wg.Done()
		}(shard, configNum)
	}
	wg.Wait()
}

func (kv *ShardKV) tick() {
	for {
		select {
		case <-kv.pullConfigTimer.C:
			go kv.pullConfig()
		case <-kv.pullDataTimer.C:
			go kv.pullData()
		}
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.curConfig = shardmaster.Config{}
	kv.waitingShards = make(map[int]int)
	kv.pullConfigTimer = time.NewTimer(pullConfigInterval)
	kv.pullDataTimer = time.NewTimer(pullDataInterval)
	kv.finishChans = make(map[int]chan opReply)
	kv.reqTerm = make(map[int]int)
	kv.data = make(map[string]string)
	kv.clerkBolts = make(map[int64]int64)
	kv.ownShards = make(map[int]struct{})
	kv.ownShardsConfigNum = make(map[int]int)
	kv.pendingData = make(map[int]map[int]map[string]string)
	kv.waitingShards = make(map[int]int)
	go kv.tick()
	go kv.apply()
	return kv
}
