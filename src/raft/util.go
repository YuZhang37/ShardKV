package raft

import "log"

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
const colorRed = "\033[0;31m"

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
