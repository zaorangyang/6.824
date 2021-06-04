package raftkv

import (
	"crypto/rand"
	"math/big"
	"time"

	"github.com/zaorangyang/6.824/labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	clerkID  int64
	opID     int64
	leaderID int64
}

func getClerkID() int64 {
	return time.Now().UnixNano()
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) getMasterIDRandomly() int64 {
	return nrand() % int64(len(ck.servers))
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkID = getClerkID()
	ck.leaderID = ck.getMasterIDRandomly()
	return ck
}

/* Get/Put/Append假设一个Clerk同一时刻只有一个操作在进行 */

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:     key,
		ClerkID: ck.clerkID,
		OpID:    ck.opID,
	}
	defer func() {
		ck.opID++
	}()
	op := "Get"
	for {
		reply := GetReply{}
		leaderID := ck.leaderID
		ok := ck.servers[leaderID].Call("KVServer.Get", &args, &reply)
		if !ok {
			DPrintf("%s rpc call error, leaderID=%v, clearkID=%v, opID= %v", op, leaderID, args.ClerkID, args.OpID)
			continue
		}
		if !reply.WrongLeader && len(reply.Err) == 0 {
			return reply.Value
		}
		if reply.WrongLeader {
			DPrintf("%s wrong leader, leaderID=%v, clearkID=%v, opID= %v", op, leaderID, args.ClerkID, args.OpID)
			newMasterID := ck.getMasterIDRandomly()
			ck.leaderID = newMasterID
		} else {
			DPrintf("%s error: %s, leaderID=%v, clearkID=%v, opID= %v", op, reply.Err, leaderID, args.ClerkID, args.OpID)
		}
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:     key,
		Value:   value,
		ClerkID: ck.clerkID,
		OpID:    ck.opID,
		Op:      op,
	}
	defer func() {
		ck.opID++
	}()

	for {
		reply := PutAppendReply{}
		leaderID := ck.leaderID
		ok := ck.servers[leaderID].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			DPrintf("%s rpc call error, leaderID=%v, clearkID=%v, opID= %v", op, leaderID, args.ClerkID, args.OpID)
			newMasterID := ck.getMasterIDRandomly()
			ck.leaderID = newMasterID
			continue
		}
		if !reply.WrongLeader && len(reply.Err) == 0 {
			return
		}
		if reply.WrongLeader {
			DPrintf("%s wrong leader, leaderID=%v, clearkID=%v, opID= %v", op, leaderID, args.ClerkID, args.OpID)
			newMasterID := ck.getMasterIDRandomly()
			ck.leaderID = newMasterID
		} else {
			// 如果超时会**重复**向同一个节点请求
			DPrintf("%s error: %s, leaderID=%v, clearkID=%v, opID= %v", op, reply.Err, leaderID, args.ClerkID, args.OpID)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
