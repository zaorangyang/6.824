package raft

import (
	"log"
)

// Debugging
const Debug = 1

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

func newRaftLog(capacity uint64) *raftLog {
	log := &raftLog{
		capacity: capacity,
		used:     0,
		in:       1,
		out:      1,
		log:      make([]*LogEntry, capacity),
	}
	log.log[0] = &LogEntry{
		Term: 0,
	}
	return log
}

func (log *raftLog) getLastLogEntry() (*LogEntry, uint64) {
	return log.log[(log.in-1)%log.capacity], log.in - 1
}

func (log *raftLog) getLogEntryByIndex(index uint64) *LogEntry {
	return log.log[index%log.capacity]
	//if log.used <= 0 {
	//	return nil
	//}
	//if index >= log.out && index <= log.in {
	//	return log.log[index%log.capacity]
	//}
	//if log.in < log.out {
	//	if index >= log.out || index <= log.in {
	//		return log.log[index%log.capacity]
	//	}
	//}
}

func (log *raftLog) getLogEntryByRange(from uint64, to uint64) []*LogEntry {
	entries := []*LogEntry{}
	for i := from; i < to; i++ {
		entries = append(entries, log.log[i])
	}
	return entries
}

// 删除index+1到in的所有日志
func (log *raftLog) deleteEntriesByIndex(index uint64) {
	log.in = index + 1
}

func (log *raftLog) appendEntries(entries []*LogEntry) {
	for _, entry := range entries {
		log.log[log.in] = entry
		log.in++
	}
}
