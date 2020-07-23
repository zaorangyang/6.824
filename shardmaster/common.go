package shardmaster

import (
	"fmt"
	"log"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (config *Config) Copy() Config {
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

func getConfigStr(config Config) string {
	return fmt.Sprintf("[Num=%v, Shards=%v, Groups=%v, GroupsNum=%v]", config.Num, config.Shards, config.Groups, len(config.Groups))
}

func ConfigCheck(config Config) (bool, string) {
	occuredGids := make(map[int]struct{})
	// shard对应的gid应该在group中
	for _, gid := range config.Shards {
		_, exist := config.Groups[gid]
		if !exist {
			msg := fmt.Sprintf("shard对应的gid不在group中:%v", gid)
			return exist, msg
		}
		occuredGids[gid] = struct{}{}
	}

	// 除非有move操作，否则group的gid应该都出现在shard中
	for gid, _ := range config.Groups {
		_, exist := occuredGids[gid]
		if !exist {
			msg := fmt.Sprintf("group的gid没有出现在shard中:%v", gid)
			return exist, msg
		}
	}
	return true, ""
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ClerkID int64
	OpID    int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs    []int
	ClerkID int64
	OpID    int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard   int
	GID     int
	ClerkID int64
	OpID    int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num     int // desired config number
	ClerkID int64
	OpID    int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
