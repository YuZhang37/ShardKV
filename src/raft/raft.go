package raft

/*
if the command sends to the valid leader, entry will be appended to the leader's local log and returns index, term, true,
an AppendCommand goroutine is issued to commit the command
otherwise returns -1, -1, false
if log exceeds maxLogSize and can't reduce size by snapshot()
return -1, -1, true. In this case, start won't return immediately
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || rf.role != LEADER {
		AppendEntriesDPrintf("Command %v sends to %v, which is not a leader, the leader is %v\n", command, rf.me, rf.currentLeader)
		return -1, -1, false
	}

	AppendEntriesDPrintf("Command %v sends to %v, which is a leader for term: %v\n", command, rf.me, rf.currentTerm)
	AppendEntriesDPrintf("Start processing...\n")

	//append to the leader's local log
	index := 0
	if len(rf.log) == 0 {
		index = rf.snapshotLastIndex + 1
	} else {
		index = rf.log[len(rf.log)-1].Index + 1
	}
	entry := LogEntry{
		Term:    rf.currentTerm,
		Index:   index,
		Command: command,
	}
	newLog := append(rf.log, entry)
	size := rf.getLogSize(newLog)
	if size >= rf.maxLogSize {
		firstEntry := LogEntry{}
		lastEntry := LogEntry{}
		if len(rf.log) > 0 {
			firstEntry = rf.log[0]
			lastEntry = rf.log[len(rf.log)-1]
		}
		rf.appliedLock.Lock()
		lastApplied := rf.lastApplied
		rf.appliedLock.Unlock()
		KVStoreDPrintf("from leader: before signalSnapshot:\n rf.me: %v, rf.role: %v, rf.appliedIndex: %v, rf.commitIndex: %v, rf.snapshotLastIndex: %v, rf.snapshotLastTerm: %v, logsize: %v, first log entry: %v, last log entry: %v\n", rf.me, rf.role, lastApplied, rf.commitIndex, rf.snapshotLastIndex, rf.snapshotLastTerm, len(rf.log), firstEntry, lastEntry)
		rf.signalSnapshot()
		rf.appliedLock.Lock()
		lastApplied = rf.lastApplied
		rf.appliedLock.Unlock()
		KVStoreDPrintf("from leader: after signalSnapshot:\n rf.me: %v, rf.role: %v, rf.appliedIndex: %v, rf.commitIndex: %v, rf.snapshotLastIndex: %v, rf.snapshotLastTerm: %v, logsize: %v, first log entry: %v, last log entry: %v\n", rf.me, rf.role, lastApplied, rf.commitIndex, rf.snapshotLastIndex, rf.snapshotLastTerm, len(rf.log), firstEntry, lastEntry)
		newLog = append(rf.log, entry)
		size = rf.getLogSize(newLog)
		if size >= rf.maxLogSize {
			return -1, -1, true
		}
	}
	rf.log = newLog
	rf.persistState("Start()")
	AppendEntriesDPrintf("Command %v is appended on %v at index of %v\n", command, rf.me, len(rf.log))

	go rf.reachConsensus(entry.Index)
	return entry.Index, entry.Term, true
}

/*
the service using Raft (e.g. a k/v server) wants to start
agreement on the next command to be appended to Raft's log. if this
server isn't the leader, returns false. otherwise start the
agreement and return immediately. there is no guarantee that this
command will ever be committed to the Raft log, since the leader
may fail or lose an election. even if the Raft instance has been killed,
this function should return gracefully.

the first return value is the index that the command will appear at
if it's ever committed. the second return value is the current
term. the third return value is true if this server believes it is
the leader.

if the command sends to the valid leader, entry will be appended to the leader's local log and returns index, term, true,
an AppendCommand goroutine is issued to commit the command
otherwise returns -1, -1, false
if log exceeds maxLogSize and can't reduce size by snapshot()
return -1, -1, true. In this case, start won't return immediately
func (rf *Raft) Start(command interface{}) (int, int, bool)
*/
