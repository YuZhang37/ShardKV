package raft

import (
	"fmt"
	"log"
	"time"
)

// const colorRed = "\033[0;31m"
const debugElection = false
const debugAppendEntries = false
const debugHeartbeat = false
const debugPersistence = false
const debugTest = false
const debugApplyCommand = false
const debugSnapshot2 = false
const debugKVStore = false
const debugShardKV = false
const debugInstallSnapshot = false
const debugElectionWins = true

const tempDebug = false
const followerDebug = false

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

func (rf *Raft) tempDPrintf(format string, a ...interface{}) (n int, err error) {
	if tempDebug {
		votedFor := int(rf.GetVotedFor())
		prefix := fmt.Sprintf("rf.gid: %v, rf.me: %v, ", rf.gid, rf.me)
		if votedFor == rf.me || followerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func (rf *Raft) electionDPrintf(format string, a ...interface{}) (n int, err error) {
	if debugElection {
		votedFor := int(rf.GetVotedFor())
		prefix := fmt.Sprintf("rf.gid: %v, rf.me: %v, ", rf.gid, rf.me)
		if votedFor == rf.me || followerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func (rf *Raft) electionWinsDPrintf(format string, a ...interface{}) (n int, err error) {
	if debugElectionWins {
		votedFor := int(rf.GetVotedFor())
		prefix := fmt.Sprintf("rf.gid: %v, rf.me: %v, ", rf.gid, rf.me)
		if votedFor == rf.me || followerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func (rf *Raft) appendEntriesDPrintf(format string, a ...interface{}) (n int, err error) {
	if debugAppendEntries {
		votedFor := int(rf.GetVotedFor())
		prefix := fmt.Sprintf("rf.gid: %v, rf.me: %v, ", rf.gid, rf.me)
		if votedFor == rf.me || followerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func (rf *Raft) appendEntries2DPrintf(funct int, format string, a ...interface{}) (n int, err error) {
	if funct == 1 && debugAppendEntries {
		votedFor := int(rf.GetVotedFor())
		prefix := fmt.Sprintf("rf.gid: %v, rf.me: %v, ", rf.gid, rf.me)
		if votedFor == rf.me || followerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	if funct == 2 && debugHeartbeat {
		votedFor := int(rf.GetVotedFor())
		prefix := fmt.Sprintf("rf.gid: %v, rf.me: %v, ", rf.gid, rf.me)
		if votedFor == rf.me || followerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func (rf *Raft) persistenceDPrintf(format string, a ...interface{}) (n int, err error) {
	if debugPersistence {
		votedFor := int(rf.GetVotedFor())
		prefix := fmt.Sprintf("rf.gid: %v, rf.me: %v, ", rf.gid, rf.me)
		if votedFor == rf.me || followerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func (rf *Raft) testDPrintf(format string, a ...interface{}) (n int, err error) {
	if debugTest {
		votedFor := int(rf.GetVotedFor())
		prefix := fmt.Sprintf("rf.gid: %v, rf.me: %v, ", rf.gid, rf.me)
		if votedFor == rf.me || followerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func (rf *Raft) snapshot2DPrintf(format string, a ...interface{}) (n int, err error) {
	if debugSnapshot2 {
		votedFor := int(rf.GetVotedFor())
		prefix := fmt.Sprintf("rf.gid: %v, rf.me: %v, ", rf.gid, rf.me)
		if votedFor == rf.me || followerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func (rf *Raft) applyCommandDPrintf(format string, a ...interface{}) (n int, err error) {
	if debugApplyCommand {
		votedFor := int(rf.GetVotedFor())
		prefix := fmt.Sprintf("rf.gid: %v, rf.me: %v, ", rf.gid, rf.me)
		if votedFor == rf.me || followerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func (rf *Raft) kvStoreDPrintf(format string, a ...interface{}) (n int, err error) {
	if debugKVStore {
		votedFor := int(rf.GetVotedFor())
		prefix := fmt.Sprintf("rf.gid: %v, rf.me: %v, ", rf.gid, rf.me)
		if votedFor == rf.me || followerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func (rf *Raft) snapshotDPrintf(format string, a ...interface{}) (n int, err error) {
	if debugShardKV && int(rf.votedFor) == rf.me {
		prefix := fmt.Sprintf("rf.gid: %v, rf.me: %v ", rf.gid, rf.me)
		log.Printf(prefix+format, a...)
	}
	return
}

func (rf *Raft) logRaftStateForInstallSnapshot(msg string) {
	if !debugInstallSnapshot {
		return
	}
	rf.lockApplied("logRaftStateForInstallSnapshot() with msg: %v", msg)
	lastApplied := rf.lastApplied
	rf.unlockApplied()
	log.Printf("%v:\n rf.gid: %v, rf.me: %v, rf.role: %v,\n rf.appliedIndex: %v, rf.commitIndex: %v, rf.snapshotLastIndex: %v, rf.snapshotLastTerm: %v, \n log entries (size %v): %v\n", msg, rf.gid, rf.me, rf.role, lastApplied, rf.commitIndex, rf.snapshotLastIndex, rf.snapshotLastTerm, len(rf.log), rf.log)
}

func (rf *Raft) debugInstallSnapshot(format string, a ...interface{}) (n int, err error) {
	if debugInstallSnapshot {
		prefix := fmt.Sprintf("rf.gid: %v, rf.me: %v ", rf.gid, rf.me)
		log.Printf(prefix+format, a...)
	}
	return
}

func (rf *Raft) logRaftState(msg string) {
	if !debugKVStore {
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
	rf.kvStoreDPrintf("rf.gid: %v, rf.me: %v, size of new log: %v, exceeds maxLogsize: %v rf.commitIndex: %v, rf.snapshotLstIndex: %v, will snapshot: %v\n", rf.gid, rf.me, size, rf.maxLogSize, rf.commitIndex, rf.snapshotLastIndex, rf.commitIndex > rf.snapshotLastIndex)
}
