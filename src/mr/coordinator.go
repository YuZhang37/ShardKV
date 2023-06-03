package mr

import (
	// "fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	// for mapTaskStatus and reduceTaskStatus
	STATUS_NOT_ISSUED = 0
	STATUS_ISSUED     = 1
	STATUS_COMPLETED  = 2
	STATUS_FAILED     = 3

	// for taskType
	TASK_TYPE_MAP    = 1
	TASK_TYPE_REDUCE = 2
	TASK_TYPE_DONE   = 3
)

type Coordinator struct {
	taskType  int
	timeLimit int
	mu        sync.Mutex

	// states for map
	filenames        []string
	mapTaskStatus    []int
	finishedMapTasks int

	// states for reduce
	nReduce             int
	reduceTaskStatus    []int
	finishedReduceTasks int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) InitTask(args *InitTaskArgs, reply *InitTaskReply) error {
	// fmt.Println("InitTask is call.")
	reply.BucketCount = c.nReduce
	reply.MapTaskCount = len(c.filenames)
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// fmt.Println("GetTask is call.")
	reply.TaskType = WAIT_TASK
	c.mu.Lock()
	if c.taskType == TASK_TYPE_MAP {
		// fmt.Println("mapTaskStatus: ")
		// for i, ele := range c.mapTaskStatus {
		// 	fmt.Printf("(%v, %v) ", i, ele)
		// }
		for i, ele := range c.mapTaskStatus {
			if ele == STATUS_NOT_ISSUED || ele == STATUS_FAILED {
				c.mapTaskStatus[i] = STATUS_ISSUED
				reply.TaskType = MAP_TASK
				reply.TaskId = i + 1
				reply.TaskContent = c.filenames[i]
				break
			}
		}
	}
	if c.taskType == TASK_TYPE_REDUCE {
		// fmt.Println("reduceTaskStatus: ")
		// for i, ele := range c.reduceTaskStatus {
		// 	fmt.Printf("(%v, %v) ", i, ele)
		// }
		for i, ele := range c.reduceTaskStatus {
			if ele == STATUS_NOT_ISSUED || ele == STATUS_FAILED {
				c.reduceTaskStatus[i] = STATUS_ISSUED
				reply.TaskType = REDUCE_TASK
				reply.TaskId = i + 1
				reply.TaskContent = strconv.Itoa(i + 1)
				break
			}
		}
	}
	if reply.TaskType != WAIT_TASK {
		go c.timeTask(reply.TaskType, reply.TaskId)
	}
	c.mu.Unlock()
	// fmt.Printf("reply: TaskType: %v, TaskId: %v, TaskContent: %v\n", reply.TaskType, reply.TaskId, reply.TaskContent)
	return nil
}

func (c *Coordinator) timeTask(taskType int, taskId int) {
	time.Sleep(time.Duration(c.timeLimit) * time.Second)
	c.mu.Lock()
	if taskType == MAP_TASK {
		if c.mapTaskStatus[taskId-1] != STATUS_COMPLETED {
			c.mapTaskStatus[taskId-1] = STATUS_FAILED
		}
	} else {
		if c.reduceTaskStatus[taskId-1] != STATUS_COMPLETED {
			c.reduceTaskStatus[taskId-1] = STATUS_FAILED
		}
	}
	c.mu.Unlock()
	// fmt.Printf("time goes off: TaskType: %v, TaskId: %v,\n", taskType, taskId)
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs,
	reply *FinishTaskReply) error {
	// fmt.Println("FinishTask is call.")
	// fmt.Printf("args: TaskType: %v, TaskId: %v, TaskStatus: %v\n", args.TaskType, args.TaskId, args.TaskStatus)
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == MAP_TASK {
		if args.TaskStatus == COMPLETED_TASK {
			c.mapTaskStatus[args.TaskId-1] = STATUS_COMPLETED
			c.finishedMapTasks++
			if c.finishedMapTasks == len(c.filenames) {
				c.taskType = TASK_TYPE_REDUCE
			}
		} else {
			c.mapTaskStatus[args.TaskId-1] = STATUS_FAILED
		}
	} else {
		if args.TaskStatus == COMPLETED_TASK {
			c.reduceTaskStatus[args.TaskId-1] = STATUS_COMPLETED
			c.finishedReduceTasks++
			if c.finishedReduceTasks == c.nReduce {
				c.taskType = TASK_TYPE_DONE
			}
		} else {
			c.reduceTaskStatus[args.TaskId-1] = STATUS_FAILED
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	// fmt.Println("Server is running.")
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	c.mu.Lock()
	ret := c.taskType == TASK_TYPE_DONE
	c.mu.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.timeLimit = 10
	c.taskType = TASK_TYPE_MAP
	c.filenames = make([]string, len(files))
	c.mapTaskStatus = make([]int, len(files))
	c.finishedMapTasks = 0
	c.nReduce = nReduce
	c.reduceTaskStatus = make([]int, nReduce)
	c.finishedReduceTasks = 0
	for i, _ := range files {
		c.filenames[i] = files[i]
	}
	c.server()
	return &c
}
