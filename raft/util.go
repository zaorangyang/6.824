package raft

import (
	"errors"
	"fmt"
	"log"
	"strings"
)

// Debugging
const Debug = 1

var ServerId int32 = -1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func Min(arg1 uint64, arg2 uint64) uint64 {
	if arg1 < arg2 {
		return arg1
	}
	return arg2
}

type raftLog struct {
	capacity uint64
	used     uint64
	// 表示下一个要入队的位置，最后一条日志的位置应该为in-1
	in  uint64
	out uint64
	log []*LogEntry
}

var noLogErr = errors.New("no log")

func newRaftLog(capacity uint64) *raftLog {
	log := &raftLog{
		capacity: capacity,
		used:     1,
		in:       1,
		out:      0,
		log:      make([]*LogEntry, capacity),
	}
	log.log[0] = &LogEntry{
		Term: 0,
	}
	return log
}

func (log *raftLog) getLastLogEntryIndex() uint64 {
	return log.in - 1
}

func (log *raftLog) getLastLogEntry() (*LogEntry, uint64) {
	if log.used == 0 {
		return nil, 0
	}
	return log.log[(log.in-1)%log.capacity], log.in - 1
}

func (log *raftLog) getLogEntryByIndex(index uint64) *LogEntry {
	if index >= log.out && index < log.in {
		return log.log[index%log.capacity]
	}
	return nil
}

// 获得[from, to)区间内的日志
func (log *raftLog) getLogEntryByRange(from uint64, to uint64) []*LogEntry {
	entries := []*LogEntry{}
	if !(from >= log.out && to <= log.in) {
		return nil
	}
	for i := from; i < to; i++ {
		entries = append(entries, log.log[i])
	}
	return entries
}

// 删除preIndex+1到in的所有日志
func (log *raftLog) deleteEntriesByIndex(preIndex uint64) {
	oldIn := log.in
	log.in = preIndex + 1
	log.used -= oldIn - log.in
}

// 比较当前节点从(preLogIndex, preLogIndex+len(entries)]的日志，当前节点的日志长度不足，或者日志不匹配时返回false
func (log *raftLog) compareEntries(preLogIndex uint64, entries []*LogEntry) bool {
	curIndex := preLogIndex + 1
	for i := 0; i < len(entries); i++ {
		if curIndex >= log.in {
			return false
		}
		if log.getLogEntryByIndex(curIndex).Term != entries[i].Term {
			return false
		}
	}
	return true
}

// 追加日志切片，返回开始追加的日志位置
func (log *raftLog) appendEntries(entries []*LogEntry) uint64 {
	startIndex := log.in
	for _, entry := range entries {
		log.log[log.in] = entry
		log.in++
	}
	log.used += uint64(len(entries))
	return startIndex
}

//
func (log *raftLog) getLogStr() string {
	logStr := []string{}
	for i := log.out; i < log.in; i++ {
		logStr = append(logStr, fmt.Sprintf("(%d:%d)", i, log.log[i%log.capacity]))
	}
	return strings.Join(logStr, ",")
}
