package raftkv

import (
	"github.com/Drewryz/6.824/labgob"
	"github.com/Drewryz/6.824/labrpc"
	"github.com/Drewryz/6.824/raft"
	"log"
	"os"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const opTimeOut = 2 * time.Second

type Op struct {
	ClerkID int64
	OpID    int64
	Op      string
	Key     string
	Value   string
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	finishChans  map[int]chan string
	reqTerm      map[int]int // 记录接收每个请求时的term
	data         map[string]string
	clerkBolts   map[int64]int64
	maxraftstate int // snapshot if log grows this big
}

func (kv *KVServer) opHelper(op *Op) GetReply {
	// tricy here: 用GetReply作为通用的reply
	reply := GetReply{}
	clerkID := op.ClerkID
	opID := op.OpID
	kv.mu.Lock()
	bolt, ok := kv.clerkBolts[clerkID]
	if ok && opID < bolt {
		// TODO: 某些情况下，客户端是否会一直重试?
		reply.Err = "already executed"
		kv.mu.Unlock()
		return reply
	}
	opIndex, term, isLeader := kv.rf.Start(*op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return reply
	}
	finishChan := make(chan string, 1)
	kv.finishChans[opIndex] = finishChan
	kv.reqTerm[opIndex] = term
	kv.mu.Unlock()

	var value string
	timer := time.NewTimer(opTimeOut)
	select {
	case <-timer.C:
		reply.Err = "timeout"
	case value = <-finishChan:
	}

	kv.mu.Lock()
	delete(kv.finishChans, opIndex)
	delete(kv.reqTerm, opIndex)
	kv.mu.Unlock()

	if op.Op == "Get" {
		reply.Value = value
	}
	return reply
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) apply() {
	for {
		select {
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				DPrintf("server apply command invalid")
				continue
			}
			op, ok := msg.Command.(Op)
			if !ok {
				DPrintf("server get unrecognized msg format")
				os.Exit(1)
			}

			kv.mu.Lock()
			kv.clerkBolts[op.ClerkID] = op.OpID + 1
			var value string
			switch op.Op {
			case "Get":
				value, ok = kv.data[op.Key]
				if !ok {
					DPrintf("[server] dnot have key: %v", op.Key)
					value = ""
				}

			case "Put":
				kv.data[op.Key] = op.Value
			case "Append":
				value, ok := kv.data[op.Key]
				if !ok {
					kv.data[op.Key] = op.Value
				} else {
					kv.data[op.Key] = value + op.Value
				}
			}
			finishChan, chanExist := kv.finishChans[msg.CommandIndex]
			term, termExist := kv.reqTerm[msg.CommandIndex]
			if chanExist && termExist && term == int(msg.Term) {
				finishChan <- value
			}
			kv.mu.Unlock()

		}
	}

}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.finishChans = make(map[int]chan string)
	kv.reqTerm = make(map[int]int)
	kv.data = make(map[string]string)
	kv.clerkBolts = make(map[int64]int64)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.apply()
	return kv
}
