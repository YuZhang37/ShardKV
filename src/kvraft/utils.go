package kvraft

import (
	"log"
)

const TempDebug = false

func TempDPrintf(format string, a ...interface{}) (n int, err error) {
	if TempDebug {
		log.Printf(format, a...)
	}
	return
}
