package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "sort"
import "encoding/json"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var bucketCount int
var mapTaskCount int
var taskType int
var taskId int

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	initArgs := GetBucketInfoArgs{}
	initReply := ReplyGetBucketInfoArgs{}

	call("Coordinator.InitTask", &initArgs, &initReply)
	bucketCount = initReply.bucketCount
	mapTaskCount = initReply.mapTaskCount

	args := GetTaskArgs{}
	reply := ReplyGetTaskArgs{}

	for {
		succeeded := call("Coordinator.GetTask", &args, &reply)
		if !succeeded {
			break
		}
		if taskType == WAIT_TASK {
			time.Sleep(time.Second)
			continue
		}

		taskType = reply.taskType
		taskId = reply.taskId

		taskStatus := -1

		if taskType == MAP_TASK {
			taskStatus = runMapTask(mapf, reply.taskContent)
		} else {
			taskStatus = runReduceTask(reducef, reply.taskContent)
		}

		finishArgs := FinishTaskArgs{
			taskType:   taskType,
			taskId:     taskId,
			taskStatus: taskStatus,
		}

		replyFinishArgs := ReplyFinishTaskArgs{}

		call("Coordinator.FinishTask", &finishArgs, &replyFinishArgs)
	}

}

func runMapTask(mapf func(string, string) []KeyValue, filename string) int {
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return FAILED_TASK
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return FAILED_TASK
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	ofiles := make([]*os.File, bucketCount)
	fileEncoders := make([]*json.Encoder, bucketCount)
	for i := 0; i < bucketCount; i++ {
		oname := fmt.Sprintf("mr-%v-%v", taskId, i)
		ofiles[i], err = os.Create(oname)
		if err != nil {
			log.Fatalf("cannot create %v", oname)
			return FAILED_TASK
		}
		fileEncoders[i] = json.NewEncoder(ofiles[i])
	}

	for _, kv := range intermediate {
		index := ihash(kv.Key) % bucketCount
		err := fileEncoders[index].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", kv)
			return FAILED_TASK
		}
	}

	for i := 0; i < bucketCount; i++ {
		ofiles[i].Close()
	}

	return COMPLETED_TASK
}

func runReduceTask(reducef func(string, []string) string, bucket_no string) int {
	// since all files live on the same machine, we can just read the file
	// for the implementation in the map reduce paper,
	// all the input files to Reduce task are local to the machine running
	// the corresponding mapper
	// need to bookkeep machine info in coordinator, and pass this info
	// to reduce task, so the reduce worker can contact the machines and
	// get corresponding files

	kva := []KeyValue{}
	for i := 0; i < mapTaskCount; i++ {
		filename := fmt.Sprintf("mv-%v-%v", i, bucket_no)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", file)
			return FAILED_TASK
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	i := 0
	tmpfile, err := ioutil.TempFile("", "tmp")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	oFilename := fmt.Sprintf("mv-out-%v", taskId)
	os.Rename(tmpfile.Name(), oFilename)
	return COMPLETED_TASK
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
