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

type InitTaskArgs struct{}

type InitTaskReply struct {
	BucketCount  int
	MapTaskCount int
}

const (
	// for taskType
	WAIT_TASK   = -1
	MAP_TASK    = 1
	REDUCE_TASK = 2
	EXIT_TASK   = 3

	// for finish status
	COMPLETED_TASK = 1
	FAILED_TASK    = 2
)

type GetTaskArgs struct{}
type GetTaskReply struct {
	TaskType    int // -1: wait, 1: map, 2: reduce, ?3: no tasks
	TaskId      int
	TaskContent string // filename for map, bucket_no for reduce
}

type FinishTaskArgs struct {
	TaskType   int // 1: map, 2: reduce
	TaskId     int
	TaskStatus int // 1: completed, 2: failed
}

type FinishTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
