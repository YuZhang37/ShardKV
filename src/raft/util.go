package raft

import "log"

// Debugging
const Debug = false
const DebugElection = false
const DebugAppendEntries = false
const DebugHeartbeat = false

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
