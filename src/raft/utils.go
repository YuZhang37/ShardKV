package raft

import (
	"fmt"
	"log"
	"time"
)

// const colorRed = "\033[0;31m"
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
const DebugInstallSnapshot = false
const DebugElectionWins = true

const WatchLock = 1

func (rf *Raft) lockMu(format string, a ...interface{}) {
	rf.mu.Lock()
	if WatchLock == 1 {
		rf.lockChan = make(chan int)
		go rf.watchMuLock(format, a...)
	}
}

func (rf *Raft) unlockMu() {
	if WatchLock == 1 {
		rf.lockChan <- 1
	}
	rf.mu.Unlock()
}

func (rf *Raft) watchMuLock(format string, a ...interface{}) {
	quit := 0
	for quit != 1 {
		select {
		case <-rf.lockChan:
			quit = 1
		case <-time.After(5 * time.Second):
			prefix := fmt.Sprintf("MuLock: rf.gid: %v, rf.me: %v ", rf.gid, rf.me)
			log.Printf(prefix+"Raft testLock(): "+format+"is not unlocked\n", a...)
		}
	}
}

func (rf *Raft) lockApplied(format string, a ...interface{}) {
	rf.appliedLock.Lock()
	if WatchLock == 1 {
		rf.appliedLockChan = make(chan int)
		go rf.watchAppliedLock(format, a...)
	}
}

func (rf *Raft) unlockApplied() {
	if WatchLock == 1 {
		rf.appliedLockChan <- 1
	}
	rf.appliedLock.Unlock()
}

func (rf *Raft) watchAppliedLock(format string, a ...interface{}) {
	quit := 0
	for quit != 1 {
		select {
		case <-rf.appliedLockChan:
			quit = 1
		case <-time.After(5 * time.Second):
			prefix := fmt.Sprintf("AppliedLock: rf.gid: %v, rf.me: %v ", rf.gid, rf.me)
			log.Printf(prefix+"Raft testLock(): "+format+"is not unlocked\n", a...)
		}
	}
}

func (rf *Raft) logFatal(format string, a ...interface{}) {
	prefix := fmt.Sprintf("Fatal: Group: %v, ShardKVServer: %v, ", rf.gid, rf.me)
	log.Fatalf(prefix+format, a...)
}

func ElectionDPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugElection {
		log.Printf(format, a...)
	}
	return
}

func ElectionWinsDPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugElectionWins {
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

func (rf *Raft) logRaftStateForInstallSnapshot(msg string) {
	if !DebugInstallSnapshot {
		return
	}
	rf.lockApplied("logRaftStateForInstallSnapshot() with msg: %v", msg)
	lastApplied := rf.lastApplied
	rf.unlockApplied()
	log.Printf("%v:\n rf.gid: %v, rf.me: %v, rf.role: %v,\n rf.appliedIndex: %v, rf.commitIndex: %v, rf.snapshotLastIndex: %v, rf.snapshotLastTerm: %v, \n log entries (size %v): %v\n", msg, rf.gid, rf.me, rf.role, lastApplied, rf.commitIndex, rf.snapshotLastIndex, rf.snapshotLastTerm, len(rf.log), rf.log)
}

func (rf *Raft) debugInstallSnapshot(format string, a ...interface{}) (n int, err error) {
	if DebugInstallSnapshot {
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
	rf.lockApplied("logRaftState() with msg: %v", msg)
	lastApplied := rf.lastApplied
	rf.unlockApplied()
	log.Printf("%v:\n rf.gid: %v, rf.me: %v, rf.role: %v, rf.appliedIndex: %v, rf.commitIndex: %v, rf.snapshotLastIndex: %v, rf.snapshotLastTerm: %v, logsize: %v, first log entry: %v, last log entry: %v\n", msg, rf.gid, rf.me, rf.role, lastApplied, rf.commitIndex, rf.snapshotLastIndex, rf.snapshotLastTerm, len(rf.log), firstEntry, lastEntry)
}

func (rf *Raft) logRaftState2(size int) {
	KVStoreDPrintf("rf.gid: %v, rf.me: %v, size of new log: %v, exceeds maxLogsize: %v rf.commitIndex: %v, rf.snapshotLstIndex: %v, will snapshot: %v\n", rf.gid, rf.me, size, rf.maxLogSize, rf.commitIndex, rf.snapshotLastIndex, rf.commitIndex > rf.snapshotLastIndex)
}
