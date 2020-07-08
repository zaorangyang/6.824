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
	// TODO: Used is useless.
	Used uint64
	// 表示下一个要入队的位置，最后一条日志的位置应该为in-1
	In   uint64
	Log  []*LogEntry
	Base uint64
}

var noLogErr = errors.New("no log")

// TODO: RaftLog的指针并未考虑到溢出情况
func newRaftLog() *RaftLog {
	log := &RaftLog{
		Used: 1,
		In:   1,
		Log:  make([]*LogEntry, 0),
		Base: 0,
	}
	log.Log = append(log.Log, &LogEntry{
		Term: 0,
	})
	return log
}

// 删除(,guard]所有的日志
func (log *RaftLog) discardOldLog(guard uint64) {
	log.Log = append(log.Log[:0], log.Log[guard-log.Base+1:]...)
	log.Used -= guard - log.Base + 1
	log.Base = guard + 1
}

func (log *RaftLog) getLastLogEntryIndex() uint64 {
	return log.In - 1
}

func (log *RaftLog) getLastLogEntry() (*LogEntry, uint64) {
	if log.Used == 0 {
		return nil, 0
	}
	return log.Log[log.In-log.Base-1], log.In - 1
}

func (log *RaftLog) getLogEntryByIndex(index uint64) *LogEntry {
	if index >= log.Base && index < log.In {
		return log.Log[index-log.Base]
	}
	return nil
}

// 获得[from, to)区间内的日志
func (log *RaftLog) getLogEntryByRange(from uint64, to uint64) []*LogEntry {
	entries := []*LogEntry{}
	if !(from >= log.Base && to <= log.In) {
		return nil
	}
	for i := from; i < to; i++ {
		entries = append(entries, log.Log[i-log.Base])
	}
	return entries
}

// 删除preIndex+1到in的所有日志
func (log *RaftLog) deleteEntriesByIndex(preIndex uint64) {
	oldIn := log.In
	log.In = preIndex + 1
	log.Used -= oldIn - log.In
	log.Log = append(log.Log[:preIndex-log.Base+1], log.Log[len(log.Log):]...)
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
		curIndex++
	}
	return true
}

// 追加日志切片，返回开始追加的日志位置
func (log *RaftLog) appendEntries(entries []*LogEntry) uint64 {
	startIndex := log.In
	log.Log = append(log.Log, entries...)
	log.In += uint64(len(entries))
	log.Used += uint64(len(entries))
	return startIndex
}

//
func (log *RaftLog) getLogStr() string {
	logStr := []string{}
	for i := log.Base; i < log.In; i++ {
		logStr = append(logStr, fmt.Sprintf("(%d:%d)", i, log.Log[i-log.Base]))
	}
	return strings.Join(logStr, ",")
}
