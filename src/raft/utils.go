package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
// const Debug = false
// const DebugElection = false
// const DebugAppendEntries = false
// const DebugHeartbeat = false
// const DebugPersistence = false

// case 1:
// const Debug = true
// const DebugElection = true
// const DebugAppendEntries = false
// const DebugHeartbeat = true
// const DebugPersistence = false

// case 2:

// const Debug = true
// const DebugElection = false
// const DebugAppendEntries = true
// const DebugHeartbeat = false
// const DebugPersistence = true

// case 3:
// const	Debug = true
// const	DebugElection = true
// const	DebugAppendEntries = false
// const	DebugHeartbeat = true
// const	DebugPersistence = true
// case 4:
const Debug = false
const DebugElection = false
const DebugAppendEntries = false
const DebugHeartbeat = false
const DebugPersistence = false
const DebugTest = false
const DebugSnapshot = false
const DebugApplyCommand = false
const DebugTemp = false
const DebugSnapshot2 = false
const DebugKVStore = false
const DebugCommitNoop = false
const DebugShardController = false
const DebugShardKV = false
const DebugSnapshotLock = false

const WatchLock = false

func (rf *Raft) lockMu(format string, a ...interface{}) {
	rf.mu.Lock()
	if WatchLock {
		rf.lockChan = make(chan int)
		go rf.testLock(format, a...)
	}
}

func (rf *Raft) unlockMu() {
	if WatchLock {
		rf.lockChan <- 1
	}
	rf.mu.Unlock()
}

func (rf *Raft) testLock(format string, a ...interface{}) {
	quit := 0
	for quit != 1 {
		select {
		case <-rf.lockChan:
			quit = 1
		case <-time.After(5 * time.Second):
			rf.snapshotDPrintf("Raft testLock(): "+format+"is not unlocked", a...)
		}
	}
}

// const colorRed = "\033[0;31m"

func DebugRaft(info int) {

	log.Printf("Debug = %v, \nDebugElection = %v, \n DebugAppendEntries = %v, \n DebugHeartbeat = %v, \n DebugPersistence = %v\n", Debug, DebugElection, DebugAppendEntries, DebugHeartbeat, DebugPersistence)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func ElectionDPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugElection {
		log.Printf(format, a...)
	}
	return
}

func AppendEntriesDPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugAppendEntries {
		log.Printf(format, a...)
	}
	return
}

func HeartbeatDPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugHeartbeat {
		log.Printf(format, a...)
	}
	return
}

func AppendEntries2DPrintf(funct int, format string, a ...interface{}) (n int, err error) {
	if funct == 1 && DebugAppendEntries {
		log.Printf(format, a...)
	}
	if funct == 2 && DebugHeartbeat {
		log.Printf(format, a...)
	}
	return
}

func PersistenceDPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugPersistence {
		log.Printf(format, a...)
	}
	return
}

func TestDPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugTest {
		log.Printf("\n \033[0;31m "+format+" \n", a...)
	}
	return
}

func SnapshotDPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugSnapshot {
		log.Printf(format, a...)
	}
	return
}

func Snapshot2DPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugSnapshot2 {
		log.Printf(format, a...)
	}
	return
}

func TempDPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugTemp {
		log.Printf(format, a...)
	}
	return
}

func ApplyCommandDPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugApplyCommand {
		log.Printf(format, a...)
	}
	return
}

func KVStoreDPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugKVStore {
		log.Printf(format, a...)
	}
	return
}

func ShardControllerDPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugShardController {
		log.Printf(format, a...)
	}
	return
}

func ShardKVDPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugShardKV {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) snapshotDPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugShardKV && int(rf.votedFor) == rf.me {
		prefix := fmt.Sprintf("rf.gid: %v, rf.me: %v ", rf.gid, rf.me)
		log.Printf(prefix+format, a...)
	}
	return
}

func (rf *Raft) logRaftState(msg string) {
	if !DebugKVStore {
		return
	}
	firstEntry := LogEntry{}
	lastEntry := LogEntry{}
	if len(rf.log) > 0 {
		firstEntry = rf.log[0]
		lastEntry = rf.log[len(rf.log)-1]
	}
	rf.appliedLock.Lock()
	lastApplied := rf.lastApplied
	rf.appliedLock.Unlock()
	log.Printf("%v:\n rf.gid: %v, rf.me: %v, rf.role: %v, rf.appliedIndex: %v, rf.commitIndex: %v, rf.snapshotLastIndex: %v, rf.snapshotLastTerm: %v, logsize: %v, first log entry: %v, last log entry: %v\n", msg, rf.gid, rf.me, rf.role, lastApplied, rf.commitIndex, rf.snapshotLastIndex, rf.snapshotLastTerm, len(rf.log), firstEntry, lastEntry)
}

func (rf *Raft) logRaftState2(size int) {
	KVStoreDPrintf("rf.gid: %v, rf.me: %v, size of new log: %v, exceeds maxLogsize: %v rf.commitIndex: %v, rf.snapshotLstIndex: %v, will snapshot: %v\n", rf.gid, rf.me, size, rf.maxLogSize, rf.commitIndex, rf.snapshotLastIndex, rf.commitIndex > rf.snapshotLastIndex)
}
