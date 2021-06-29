package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		task := CallAskTask()
		switch task.TaskType {
		case MAPTASK:
			mapOperator(mapf, task)

		case REDUCETASK:
			reduceOperator(reducef, task)

		case WAIT:
			time.Sleep(time.Duration(time.Second * 60))

		case END:
			fmt.Printf("All Tasks are finished")
			return

		default:
			panic("Invalid task Type")
		}

	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//helper function for Worker
func mapOperator(mapf func(string, string) []KeyValue, mapTask *TaskInfo) {
	filename := mapTask.FileName
	file, err := os.Open(filename)
	if err != nil {
		errorstr := fmt.Sprintf("the file %v dose not exist", filename)
		panic(errorstr)
	}
	content, err := ioutil.ReadAll(file)

	if err != nil {
		errorstr := fmt.Sprintf("fail to read file %v", filename)
		panic(errorstr)
	}
	file.Close()
	nReduce := mapTask.NReduce
	intermediate := mapf(filename, string(content))
	outfiles := make([]*os.File, nReduce)
	encoderList := make([]*json.Encoder, nReduce)
	// initialize
	for idx := 0; idx < nReduce; idx++ {
		outfiles[idx], _ = ioutil.TempFile("mr-tmp", "mr-tmp-*")
		encoderList[idx] = json.NewEncoder(outfiles[idx])
	}

	// write intermediate into outfiles
	for _, kv := range intermediate {
		index := ihash(kv.Key) % nReduce
		enc := encoderList[index]
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Printf("File %v Key %v Value %v Error %v \n", mapTask.FileName, kv.Key, kv.Value, err)
			panic("fail to write kv into file")
		}
	}

	// rename the temport file into formal file.
	outPrefix := "mr-tmp/mr-" + strconv.Itoa(mapTask.Identifier) + "-"
	for idx, file := range outfiles {
		outname := outPrefix + strconv.Itoa(idx)
		oldpath := filepath.Join(file.Name())
		os.Rename(oldpath, outname)
		file.Close()
	}

	//Ack
	CallAckTask(mapTask.TaskType)

}

func reduceOperator(reducef func(string, []string) string, reduceTask *TaskInfo) {
	intermediate_prefix := "mr-tmp/mr-"
	taskId := reduceTask.Identifier
	kvList := make([]KeyValue, 0)
	for idx := 0; idx < reduceTask.NFiles; idx++ {
		filepath := intermediate_prefix + strconv.Itoa(idx) + "-" + strconv.Itoa(taskId)
		file, err := os.Open(filepath)
		if err != nil {
			fmt.Printf("File %v failed to open", filepath)
			panic("Error in open file")
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvList = append(kvList, kv)
		}
		file.Close()
	}

	// sort
	sort.Slice(kvList, func(i, j int) bool {
		return kvList[i].Key < kvList[j].Key
	})

	// compact
	outTmpFile, err := ioutil.TempFile("mr-tmp", "mr-*")
	if err != nil {
		panic("failed to create temporary file")
	}

	k := ""
	var v []string
	for idx, kv := range kvList {
		if k != kv.Key {
			if idx != 0 {
				ans := reducef(k, v)

				// write into the file
				fmt.Fprintf(outTmpFile, "%v %v\n", k, ans)
			}

			k = kv.Key
			v = make([]string, 0)

		}
		v = append(v, kv.Value)
	}

	// rename the file
	outfilename := "mr-out-" + strconv.Itoa(reduceTask.Identifier)
	os.Rename(filepath.Join(outTmpFile.Name()), outfilename)
	outTmpFile.Close()
	CallAckTask(reduceTask.TaskType)
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

func CallAskTask() *TaskInfo {
	// declare an argument structure.
	args := TaskInfo{}
	// declare a reply structure.
	reply := TaskInfo{}
	fmt.Println("Label")
	// send the RPC request, wait for the reply.
	call("Coordinator.AskTask", &args, &reply)
	fmt.Printf("Type %v\n", reply.TaskType)
	return &reply
}

func CallAckTask(task_type int) *TaskInfo {
	args := TaskInfo{
		TaskType: task_type,
	}

	reply := TaskInfo{}
	call("Coordinator.AckTask", &args, &reply)
	return &reply
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
	fmt.Println("1")
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
