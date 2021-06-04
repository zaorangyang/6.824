package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"github.com/zaorangyang/6.824/labrpc"
)

type Clerk struct {
	mu      sync.Mutex
	servers []*labrpc.ClientEnd
	clerkID int64
	opID    int64
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

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkID = getClerkID()
	return ck
}

func (ck *Clerk) getNextOpID() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.opID++
	return ck.opID
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	args.Num = num
	args.ClerkID = ck.clerkID
	args.OpID = ck.getNextOpID()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false && len(reply.Err) == 0 {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	args.Servers = servers
	args.ClerkID = ck.clerkID
	args.OpID = ck.getNextOpID()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false && len(reply.Err) == 0 {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	args.GIDs = gids
	args.ClerkID = ck.clerkID
	args.OpID = ck.getNextOpID()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false && len(reply.Err) == 0 {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	args.Shard = shard
	args.GID = gid
	args.ClerkID = ck.clerkID
	args.OpID = ck.getNextOpID()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false && len(reply.Err) == 0 {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
