package raft

import (
	"sync/atomic"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	// currentTerm := atomic.LoadUint64(&rf.currentTerm)
	// role := atomic.LoadInt32(&rf.role)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	role := rf.role
	return currentTerm, role == LEADER
}

/*
the tester doesn't halt goroutines created by Raft after each test,
but it does call the Kill() method. your code can use killed() to
check whether Kill() has been called. the use of atomic avoids the
need for a lock.

the issue is that long-running goroutines use memory and may chew
up CPU time, perhaps causing later tests to fail and generating
confusing debug output. any goroutine with a long-running loop
should call killed() to check whether it should stop.
*/
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// go func() {
	// 	time.Sleep(5 * time.Millisecond)
	// 	close(rf.orderedDeliveryChan)
	// }()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// func (rf *Raft) IncrementLastApplied(old int) {
// 	// atomic.AddInt32(&rf.lastApplied, 1)
// 	// rf.mu.Lock()
// 	// if rf.lastApplied == int32(old) {
// 	// 	rf.lastApplied++
// 	// }
// 	// rf.mu.Unlock()
// 	atomic.CompareAndSwapInt32(&rf.lastApplied, int32(old), int32(old+1))

// }

// func (rf *Raft) GetLastApplied() int32 {
// 	z := atomic.LoadInt32(&rf.lastApplied)
// 	// rf.mu.Lock()
// 	// z := rf.lastApplied
// 	// rf.mu.Unlock()
// 	return z
// }

func (rf *Raft) isLeader() bool {
	// ans := atomic.LoadInt32(&rf.role)
	// return ans == LEADER
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ans := rf.role
	return ans == LEADER
}

func (rf *Raft) GetLeaderId() int {
	// ans := atomic.LoadInt32(&rf.role)
	// return ans == LEADER
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ans := rf.currentLeader
	return ans
}

func (rf *Raft) IsValidLeader() bool {
	if rf.killed() || !rf.isLeader() {
		return false
	}
	return true
}

/*
must hold the lock rf.mu to call this function

	rf.currentTerm = term
	rf.role = FOLLOWER
	rf.votedFor = -1
	rf.currentLeader = -1
	rf.currentAppended = 0

need to call persist() since votedFor is changed
currentLeader may still need to update outside this function
*/
func (rf *Raft) onReceiveHigherTerm(term int) int {
	originalTerm := rf.currentTerm

	rf.currentTerm = term
	rf.role = FOLLOWER
	// set it to -1 is not a problem, since this leader did not receive vote from this server
	rf.votedFor = -1
	rf.currentLeader = -1
	rf.currentAppended = 0

	return originalTerm
}

/*
the target entry can exist in the log or beyond the log or below the log,
return the index in rf.log if it exists
return  -1, if below the log
return len(log), if beyond the log
this function is called with rf.mu held
*/
func (rf *Raft) findEntryWithIndexInLog(targetIndex int, log []LogEntry, measure int) int {
	if len(log) == 0 {
		if targetIndex <= measure {
			return -1
		} else {
			return 0
		}
	}
	left, right := 0, len(log)-1
	for left+1 < right {
		mid := left + (right-left)/2
		entry := log[mid]
		if entry.Index < targetIndex {
			left = mid
		} else {
			right = mid
		}
	}
	if targetIndex < log[left].Index {
		return -1
	}
	if targetIndex > log[right].Index {
		return len(log)
	}
	if targetIndex == log[left].Index {
		return left
	}
	return right
}

// the calling function holds the lock rf.mu
// no guarantee entries exist for this term
// find the first entry which has term larger than target term
func (rf *Raft) findLargerEntryIndex(term int) int {
	left, right := 0, len(rf.log)-1
	for left+1 < right {
		midIndex := left + (right-left)/2
		midEntry := rf.log[midIndex]
		if midEntry.Term <= term {
			left = midIndex
		} else {
			right = midIndex
		}
	}
	if rf.log[left].Term > term {
		return left
	}
	return right
}

// the calling function holds the lock rf.mu
// guarantee at least one entry exists for this term
// find the first entry whose term equals to the target term
func (rf *Raft) findStartIndex(term int) int {
	left, right := 0, len(rf.log)-1
	for left+1 < right {
		midIndex := left + (right-left)/2
		midEntry := rf.log[midIndex]
		if midEntry.Term < term {
			left = midIndex
		} else {
			right = midIndex
		}
	}
	if rf.log[left].Term == term {
		return left
	}
	return right
}
