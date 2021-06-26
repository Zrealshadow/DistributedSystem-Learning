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
	taskType   int // MAP | REDUCE
	beginTime  time.Time
	fileName   string
	identifier int
	nReduce    int
	nFiles     int
}

func (task *TaskInfo) isOutOfTime() bool {
	// larger than 60 s  We consider it is out of time
	return time.Now().Sub(task.beginTime) > time.Duration(time.Second*60)
}

func (task *TaskInfo) setNow() {
	task.beginTime = time.Now()
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
		q.unlock()
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
		if id == task.identifier {
			q.taskArray = append(q.taskArray[:idx], q.taskArray[idx+1:]...)
			break
		}
	}
	q.unlock()
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

func (c *Coordinator) AskTask(args *ExampleArgs, reply *TaskInfo) error {
	//check any task which is not finished
	if c.Done() {
		reply = nil
		return nil
	}

	// distribute map task
	task, ok := c.mapWaitingTaskList.pop()
	if ok == nil {
		task.setNow()
		c.mapTaskList.push(task)

		*reply = task
		fmt.Printf("Distribute Map Task whose identifier %v\n", reply.identifier)
		return nil
	}

	task, ok = c.reduceWaitingTaskList.pop()
	if ok == nil {
		task.setNow()
		c.reduceTaskList.push(task)
		*reply = task
		fmt.Printf("Distribute Reduce Task whose identifier %v\n", reply.identifier)
		return nil
	}

	// no extra task to distribute, but some tasks are running, maybe it will crash and redistribute
	// set reply type = WAIT
	if c.mapTaskList.size() != 0 || c.reduceTaskList.size() != 0 {
		reply.taskType = WAIT
		return nil
	}

	// all task is finished
	reply.taskType = END
	c.finished = true
	return nil
}

func (c *Coordinator) AckTask(args *TaskInfo, reply *TaskInfo) error {
	switch args.taskType {
	case MAPTASK:
		c.mapTaskList.remove(args.identifier)
		fmt.Printf("Map Task (identifier %v) complete", args.identifier)
		// If all map tasks are done, We need to start to distribute reduce Task
		if c.mapTaskList.size() == 0 && c.mapWaitingTaskList.size() == 0 {
			fmt.Printf("Map Tasks are all finished | Reduce Tasks start ")
			c.distributeReduce()
		}
	case REDUCETASK:
		fmt.Printf("Reduce Task (identifier %v) complete", args.identifier)
		c.reduceTaskList.remove(args.identifier)

	default: // END and WAIT no Need to Call AckTask
		return errors.New("MisTake in Calling AckTask")
	}
	return nil
}

func (c *Coordinator) distributeReduce() error {

	reduceTask := TaskInfo{
		taskType: REDUCETASK,
		nReduce:  c.nReduce,
		nFiles:   len(c.fileList),
	}

	for reduceIndex := 0; reduceIndex < c.nReduce; reduceIndex++ {
		task := reduceTask
		task.identifier = reduceIndex
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
			taskType:   MAPTASK,
			fileName:   filename,
			identifier: fileIndex,
			nReduce:    nReduce,
		}
		initMapTasks = append(initMapTasks, mapTask)
	}

	c := Coordinator{
		nReduce:  nReduce,
		fileList: files,
	}

	c.mapTaskList.pushList(initMapTasks)

	go c.MoveOutOtTimeTask()
	c.server()
	return &c
}
