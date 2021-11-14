package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"strconv"
	"sync"
	"time"
)

type status int32

// constant for TaskFlag
const (
	IDLE = 1 << iota
	IN_PROCESS
	COMPLETED
)

// constant for TaskType
const (
	MAPTASK = 1 << iota
	REDUCETASK
)

const (
	TIMEOUTMISSION = 10
)

type Signal struct {
	StopCh      chan struct{}
	CompletedCh chan *TaskGroup
	TimeoutCh   chan *TaskGroup
}

type Task struct {
	InputFile  string
	OutputFile string
}

type TaskGroup struct {
	TaskID int32
	// IDLE IN_PROCESS COMPLETED
	TaskFlag status
	TaskList []*Task
	// MAPTASK REDUCETASK
	TaskType int32
}

type CounterClock struct {
	Counter int
}

func (C *CounterClock) Done() {
	C.Counter -= 1
}

func (C *CounterClock) Add(total int) {
	C.Counter += total
}

func (C *CounterClock) Wait() bool {
	switch {
	case C.Counter != 0:
		return true
	default:
		return false
	}
}

type Clock struct {
	TimeoutReceiver  <-chan time.Time
	TimeoutTaskGroup *TaskGroup
}

type Coordinator struct {
	TaskGroupList  []*TaskGroup
	NReduce        int
	SignalHandler  Signal
	Mutex          sync.Mutex
	MapTaskCounter *CounterClock
}

func (c *Coordinator) AssignedTask(request *Request, response *Response) error {

	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	var assignedTask *TaskGroup
	for _, task := range c.TaskGroupList {
		if task.TaskFlag == IDLE && task.TaskType == REDUCETASK && c.MapTaskCounter.Wait() {
			continue
		} else if task.TaskFlag == IDLE && task.TaskType == MAPTASK {
			assignedTask = task
			break
		} else if task.TaskFlag == IDLE && task.TaskType == REDUCETASK {
			assignedTask = task
			break
		}
	}
	log.Printf("Counter %v", c.MapTaskCounter.Counter)
	if assignedTask == nil {
		// log.Printf("Worker %v asking for Task but no task to assign", request.WorkerInfo.WorkerName)
		response.MessageType = NO_TASK_TO_ASSIGN
	} else {
		// log.Printf("Worker %v asking for Task", request.WorkerInfo.WorkerName)
		// Assign MapTask Or ReduceTask When all MapTask completed
		response.AssignedTask = assignedTask
		response.MessageType = TASK_ASSIGN
		response.NReduce = c.NReduce
		// Mark the assignedTask TaskFLAG to IN_PROCESS
		assignedTask.TaskFlag = IN_PROCESS
		go func(regisTaskGroup *TaskGroup) {
			<-time.NewTimer(time.Second * 10).C
			c.Mutex.Lock()
			if regisTaskGroup.TaskFlag == COMPLETED {
				c.Mutex.Unlock()
				return
			}
			log.Printf("TaskID %v TaskType %v Timeout", regisTaskGroup.TaskID, regisTaskGroup.TaskType)
			c.Mutex.Unlock()
			c.SignalHandler.TimeoutCh <- regisTaskGroup
		}(assignedTask)
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
	<-c.SignalHandler.StopCh
	log.Printf("The entire job has finished")

	return true
}

func JobCompletedHandler(c *Coordinator) error {
	log.Printf("JobCompletedHandler Start")
	for {
		time.Sleep(time.Second)
		counter := 0
		c.Mutex.Lock()
		for _, task := range c.TaskGroupList {
			if task.TaskFlag == COMPLETED {
				counter += 1
			}
		}
		if counter == len(c.TaskGroupList) {
			c.SignalHandler.StopCh <- struct{}{}
		}
		c.Mutex.Unlock()
	}
}

func (c *Coordinator) TaskCompletedHandler(request *Request, response *Response) error {
	completedTask := request.CompletedTask
	c.Mutex.Lock()

	// mark the completed task flag from IN_PROCESS to COMPLETED
	for _, task := range c.TaskGroupList {
		if task.TaskID == completedTask.TaskID && task.TaskType == completedTask.TaskType {
			task.TaskFlag = COMPLETED
			if task.TaskType == MAPTASK {
				c.MapTaskCounter.Done()
			}
			break
		}
	}
	c.Mutex.Unlock()
	response.MessageType = COORDINATOR_RECEIVE
	return nil
}

func (c *Coordinator) ConstructTaskGroupList(files []string, nReduce int) {
	fileslen := len(files)
	// workDir, _ := os.Getwd()
	workDir := ""
	for i := 0; i < fileslen; i++ {
		var taskGroup *TaskGroup = &TaskGroup{}

		taskGroup.TaskID = int32(i)
		taskGroup.TaskFlag = IDLE
		taskGroup.TaskList = make([]*Task, 1)
		taskGroup.TaskType = MAPTASK

		InputFilePath := path.Join(workDir, files[i])

		task := Task{InputFile: InputFilePath}
		taskGroup.TaskList[0] = &task
		c.TaskGroupList[i] = taskGroup

	}

	for i := 0; i < nReduce; i++ {
		var taskGroup *TaskGroup = &TaskGroup{}

		taskGroup.TaskID = int32(i)
		taskGroup.TaskFlag = IDLE
		taskGroup.TaskList = make([]*Task, fileslen)
		taskGroup.TaskType = REDUCETASK

		for j := 0; j < fileslen; j++ {
			InputFilePath := path.Join(workDir, "mr-"+strconv.Itoa(j)+"-"+strconv.Itoa(i))
			task := Task{InputFile: InputFilePath}
			taskGroup.TaskList[j] = &task
		}
		c.TaskGroupList[i+fileslen] = taskGroup

	}
}

func Debug(c *Coordinator, totalTask int) error {
	log.Println("DEBUG INFO")
	for i := 0; i < totalTask; i++ {
		taskGroup := c.TaskGroupList[i]
		log.Printf("TaskID %v TaskFlag %v TaskType %v", taskGroup.TaskID, taskGroup.TaskFlag, taskGroup.TaskType)
		taskList := taskGroup.TaskList
		for _, task := range taskList {
			log.Printf("Inputfile %v", task.InputFile)
		}
		log.Printf("\n")
	}
	return nil
}

func (c *Coordinator) timeoutHandler() {
	for taskGroup := range c.SignalHandler.TimeoutCh {
		c.Mutex.Lock()
		log.Printf("Recover TaskID %v TaskType %v", taskGroup.TaskID, taskGroup.TaskType)
		for _, taskGp := range c.TaskGroupList {
			if taskGp.TaskID == taskGroup.TaskID && taskGp.TaskType == taskGroup.TaskType {
				taskGp.TaskFlag = IDLE
				break
			}
		}
		c.Mutex.Unlock()
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskGroupList: make([]*TaskGroup, len(files)+nReduce),
		NReduce:       nReduce,
		SignalHandler: Signal{
			StopCh:      make(chan struct{}),
			CompletedCh: make(chan *TaskGroup, nReduce),
			TimeoutCh:   make(chan *TaskGroup, TIMEOUTMISSION),
		},
		MapTaskCounter: &CounterClock{
			Counter: len(files),
		},
	}

	c.ConstructTaskGroupList(files, nReduce)
	// when the entire job completed,
	// exit the coordinator
	go JobCompletedHandler(&c)
	go c.timeoutHandler()
	// Debug(&c, len(files)+nReduce)
	c.server()
	return &c
}
