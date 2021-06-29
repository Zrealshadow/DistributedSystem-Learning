package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

//Define Task Type and Base Data Structure
// the interface for map task and reduce task

const (
	MAPTASK    = 1
	REDUCETASK = 2
	WAIT       = 3
	END        = 4
)

type TaskInfo struct {
	TaskType   int // MAP | REDUCE
	BeginTime  time.Time
	FileName   string
	Identifier int
	NReduce    int
	NFiles     int
}

func (task *TaskInfo) isOutOfTime() bool {
	// larger than 60 s  We consider it is out of time
	return time.Now().Sub(task.BeginTime) > time.Duration(time.Second*60)
}

func (task *TaskInfo) setNow() {
	task.BeginTime = time.Now()
}

// the array of Task in coordinator
type TaskQueue struct {
	mutex     sync.Mutex
	taskArray []TaskInfo
}

// Some method for TaskQueue (First in First Out)
func (q *TaskQueue) lock() {
	q.mutex.Lock()
}

func (q *TaskQueue) unlock() {
	q.mutex.Unlock()
}

func (q *TaskQueue) size() int {
	return len(q.taskArray)
}

// func (q * TaskQueue) isEmpty() bool {
// 	return true
// }
func (q *TaskQueue) push(task TaskInfo) {
	q.lock()
	defer q.unlock()
	q.taskArray = append(q.taskArray, task)
}

func (q *TaskQueue) pushList(taskList []TaskInfo) {
	q.lock()
	defer q.unlock()
	q.taskArray = append(q.taskArray, taskList...)
}

func (q *TaskQueue) pop() (TaskInfo, error) {
	q.lock()
	defer q.unlock()
	if q.size() == 0 {
		// q.unlock()
		return TaskInfo{}, errors.New("the task queue is empty")
	}
	ret := q.taskArray[0]
	q.taskArray = q.taskArray[1:]
	return ret, nil
}

func (q *TaskQueue) remove(id int) {
	q.lock()
	defer q.unlock()

	for idx := 0; idx < len(q.taskArray); idx++ {
		task := q.taskArray[idx]
		if id == task.Identifier {
			q.taskArray = append(q.taskArray[:idx], q.taskArray[idx+1:]...)
			break
		}
	}
}

func (q *TaskQueue) queueOutOfTime() ([]TaskInfo, error) {
	outArray := make([]TaskInfo, 0)
	q.lock()
	defer q.unlock()

	for idx := 0; idx < len(q.taskArray); {
		task := q.taskArray[idx]
		if task.isOutOfTime() {
			outArray = append(outArray, task)
			q.taskArray = append(q.taskArray[:idx], q.taskArray[idx+1:]...)
		} else {
			idx += 1
		}
	}

	return outArray, nil
}

type Coordinator struct {
	// Your definitions here.
	fileList []string
	nReduce  int

	mapTaskList    TaskQueue
	reduceTaskList TaskQueue

	mapWaitingTaskList    TaskQueue
	reduceWaitingTaskList TaskQueue

	finished bool
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

func (c *Coordinator) AskTask(args *TaskInfo, reply *TaskInfo) error {
	//check any task which is not finished
	fmt.Println("1")
	if c.Done() {
		// fmt.Println("1")
		reply = nil
		return nil
	}

	// distribute map task
	task, ok := c.mapWaitingTaskList.pop()

	if ok == nil {
		task.setNow()
		c.mapTaskList.push(task)

		*reply = task
		fmt.Printf("Distribute Map Task whose identifier %v\n", reply.Identifier)
		return nil
	}
	fmt.Println("2")
	task, ok = c.reduceWaitingTaskList.pop()
	if ok == nil {
		task.setNow()
		c.reduceTaskList.push(task)
		*reply = task
		fmt.Printf("Distribute Reduce Task whose identifier %v\n", reply.Identifier)
		return nil
	}

	// no extra task to distribute, but some tasks are running, maybe it will crash and redistribute
	// set reply type = WAIT
	if c.mapTaskList.size() != 0 || c.reduceTaskList.size() != 0 {
		fmt.Println("WAIT worker")
		reply.TaskType = WAIT
		return nil
	}

	// all task is finished
	reply.TaskType = END
	c.finished = true
	return nil
}

func (c *Coordinator) AckTask(args *TaskInfo, reply *TaskInfo) error {
	switch args.TaskType {
	case MAPTASK:
		c.mapTaskList.remove(args.Identifier)
		fmt.Printf("Map Task (identifier %v) complete", args.Identifier)
		// If all map tasks are done, We need to start to distribute reduce Task
		if c.mapTaskList.size() == 0 && c.mapWaitingTaskList.size() == 0 {
			fmt.Printf("Map Tasks are all finished | Reduce Tasks start ")
			c.distributeReduce()
		}
	case REDUCETASK:
		fmt.Printf("Reduce Task (identifier %v) complete", args.Identifier)
		c.reduceTaskList.remove(args.Identifier)

	default: // END and WAIT no Need to Call AckTask
		return errors.New("MisTake in Calling AckTask")
	}
	return nil
}

func (c *Coordinator) distributeReduce() error {

	reduceTask := TaskInfo{
		TaskType: REDUCETASK,
		NReduce:  c.nReduce,
		NFiles:   len(c.fileList),
	}

	for reduceIndex := 0; reduceIndex < c.nReduce; reduceIndex++ {
		task := reduceTask
		task.Identifier = reduceIndex
		c.reduceWaitingTaskList.push(task)
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
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

	// Your code here.

	return c.finished
}

func (c *Coordinator) MoveOutOtTimeTask() {
	for {
		time.Sleep(time.Duration(time.Second * 5))

		outTaskArray, ok := c.mapTaskList.queueOutOfTime()
		if ok == nil {
			c.mapWaitingTaskList.pushList(outTaskArray)
		}

		outTaskArray, ok = c.reduceTaskList.queueOutOfTime()
		if ok == nil {
			c.reduceWaitingTaskList.pushList(outTaskArray)
		}
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	initMapTasks := make([]TaskInfo, 0)
	for fileIndex, filename := range files {
		mapTask := TaskInfo{
			TaskType:   MAPTASK,
			FileName:   filename,
			Identifier: fileIndex,
			NReduce:    nReduce,
		}
		initMapTasks = append(initMapTasks, mapTask)
	}

	c := Coordinator{
		nReduce:  nReduce,
		fileList: files,
	}
	c.mapWaitingTaskList.pushList(initMapTasks)

	// create tmp directory
	dirname := "mr-tmp"
	if _, err := os.Stat(dirname); !os.IsNotExist(err) {
		// tmp directory exist // remove all
		err = os.RemoveAll(dirname)
		if err != nil {
			panic("failed to remove existed tmp directory")
		}
	}
	// create dir
	err := os.Mkdir(dirname, os.ModePerm)
	if err != nil {
		panic("failed to create tmp directory")
	}
	go c.MoveOutOtTimeTask()
	c.server()
	return &c
}
