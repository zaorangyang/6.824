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

type RaftLog struct {
	Capacity uint64
	Used     uint64
	// 表示下一个要入队的位置，最后一条日志的位置应该为in-1
	In  uint64
	Out uint64
	Log []*LogEntry
}

var noLogErr = errors.New("no log")

func newRaftLog(capacity uint64) *RaftLog {
	log := &RaftLog{
		Capacity: capacity,
		Used:     1,
		In:       1,
		Out:      0,
		Log:      make([]*LogEntry, capacity),
	}

	// 此处全部初始化的原因是因为persist的时候数组的元素不能有nil
	for i := 0; i < len(log.Log); i++ {
		log.Log[i] = &LogEntry{
			Term: 0,
		}
	}
	return log
}

func (log *RaftLog) getLastLogEntryIndex() uint64 {
	return log.In - 1
}

func (log *RaftLog) getLastLogEntry() (*LogEntry, uint64) {
	if log.Used == 0 {
		return nil, 0
	}
	return log.Log[(log.In-1)%log.Capacity], log.In - 1
}

func (log *RaftLog) getLogEntryByIndex(index uint64) *LogEntry {
	if index >= log.Out && index < log.In {
		return log.Log[index%log.Capacity]
	}
	return nil
}

// 获得[from, to)区间内的日志
func (log *RaftLog) getLogEntryByRange(from uint64, to uint64) []*LogEntry {
	entries := []*LogEntry{}
	if !(from >= log.Out && to <= log.In) {
		return nil
	}
	for i := from; i < to; i++ {
		entries = append(entries, log.Log[i])
	}
	return entries
}

// 删除preIndex+1到in的所有日志
func (log *RaftLog) deleteEntriesByIndex(preIndex uint64) {
	oldIn := log.In
	log.In = preIndex + 1
	log.Used -= oldIn - log.In
}

// 比较当前节点从(preLogIndex, preLogIndex+len(entries)]的日志，当前节点的日志长度不足，或者日志不匹配时返回false
func (log *RaftLog) compareEntries(preLogIndex uint64, entries []*LogEntry) bool {
	curIndex := preLogIndex + 1
	for i := 0; i < len(entries); i++ {
		if curIndex >= log.In {
			return false
		}
		if log.getLogEntryByIndex(curIndex).Term != entries[i].Term {
			return false
		}
	}
	return true
}

// 追加日志切片，返回开始追加的日志位置
func (log *RaftLog) appendEntries(entries []*LogEntry) uint64 {
	startIndex := log.In
	for _, entry := range entries {
		log.Log[log.In] = entry
		log.In++
	}
	log.Used += uint64(len(entries))
	return startIndex
}

//
func (log *RaftLog) getLogStr() string {
	logStr := []string{}
	for i := log.Out; i < log.In; i++ {
		logStr = append(logStr, fmt.Sprintf("(%d:%d)", i, log.Log[i%log.Capacity]))
	}
	return strings.Join(logStr, ",")
}
