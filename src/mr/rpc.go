package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

const (
	// for taskType
	WAIT_TASK   = 0
	MAP_TASK    = 1
	REDUCE_TASK = 2
	NO_TASK     = 3

	// for finish status
	COMPLETED_TASK = 1
	FAILED_TASK    = 2
)

type GetTaskArgs struct{}
type ReplyGetTaskArgs struct {
	taskType    int // 0: wait, 1: map, 2: reduce, ?3: no tasks
	taskId      int
	taskContent string // filename for map, bucket_no for reduce
}

type GetBucketInfoArgs struct{}

type ReplyGetBucketInfoArgs struct {
	bucketCount  int
	mapTaskCount int
}

type FinishTaskArgs struct {
	taskType   int // 1: map, 2: reduce
	taskId     int
	taskStatus int // 0: completed, -1: failed
}

type ReplyFinishTaskArgs struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
