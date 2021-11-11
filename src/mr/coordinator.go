package mr

import (
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
	// relevant to Task kind
	MAPTASK    int32 = 1
	REDUCETASK int32 = 1 << 1
)

type Task struct {
	TaskIdentify       string
	TaskSize           int64
	InputFile          string
	OutputFile         string
	TaskClassification int32
}

type Coordinator struct {
	Mutex            sync.Mutex
	WaitMapGroup     sync.WaitGroup
	Task_Idle        []*Task
	Task_IN_Progress []*Task
	Task_Completed   []*Task
	NReduce          int
	stopCh           chan struct{}
}

func (c *Coordinator) CoordinatorCompletedHandler() error {
	for {
		time.Sleep(time.Second)
		c.Mutex.Lock()
		if len(c.Task_IN_Progress) == 0 && len(c.Task_Idle) == 0 {
			close(c.stopCh)
		}
		c.Mutex.Unlock()
	}
}

//
// AssignTasks assign task to worker who asks for task through rpc
//
func (c *Coordinator) AssignTasks(request *Request, response *Response) error {
	c.Mutex.Lock()
	if len(c.Task_Idle) != 0 {
		if c.Task_Idle[0].TaskClassification == REDUCETASK {
			c.Mutex.Unlock()
			c.WaitMapGroup.Wait()
			c.Mutex.Lock()
			response.Message = TASKSETS
			response.NReduce = c.NReduce
			response.TaskSet, _ = c.AssignedTaskSet()
		} else {

			// Task assign to worker
			log.Printf("Worker-%v requests for task", request.Name)

			response.AssignedTask = c.Task_Idle[0]
			response.Message = NORMAL
			response.NReduce = c.NReduce

			// Remove Task From Idle and add To IN_Process
			c.Task_IN_Progress = append(c.Task_IN_Progress, c.Task_Idle[0])
			c.Task_Idle = c.Task_Idle[1:]
		}

	} else {
		response.AssignedTask = nil
		response.Message = NO_RESOURCES
	}
	c.Mutex.Unlock()
	return nil
}

func (c *Coordinator) AssignedTaskSet() ([]*Task, error) {
	var tasksSet []*Task
	reduceID := c.Task_Idle[0].TaskIdentify
	for _, task := range c.Task_Idle {
		if task.TaskIdentify == reduceID {
			tasksSet = append(tasksSet, task)
			c.Task_IN_Progress = append(c.Task_IN_Progress, task)
		}
	}
	for _, task := range tasksSet {
		for index, idle_task := range c.Task_Idle {
			if idle_task == task {
				c.Task_Idle = append(c.Task_Idle[0:index], c.Task_Idle[index+1:]...)
				break
			}
		}
	}
	return tasksSet, nil
}

//
// TaskCompletedSignalHandler will be called by the worker who has
// completed it's task
//
func (c *Coordinator) TaskCompletedSignalHandler(request *Request, response *Response) error {
	c.Mutex.Lock()
	completedTask := request.CompletedTask
	response.Message = SUCCESSFUL_RECEIVE
	if completedTask.TaskClassification == MAPTASK {
		c.WaitMapGroup.Done()
	}
	// remove completedTask From IN_progress queue and
	// adding it to the Completed_Task

	for index, task := range c.Task_IN_Progress {
		// confirm the index of completed task in IN_process queue
		// Bad implementation
		if task.InputFile == completedTask.InputFile {
			log.Printf("Remove Task From In_progress list")
			c.Task_Completed = append(c.Task_Completed, task)
			c.Task_IN_Progress = append(c.Task_IN_Progress[0:index], c.Task_IN_Progress[index+1:]...)

			break
		}
	}
	c.Mutex.Unlock()
	return nil
}

func (c *Coordinator) AddReduceTask(request *Request, response *Response) error {
	log.Printf("Adding new ReduceTask %v From worker %v", request.CompletedTask.TaskIdentify, request.Name)
	c.Mutex.Lock()
	c.Task_Idle = append(c.Task_Idle, request.CompletedTask)
	response.Message = SUCCESSFUL_ADDINGTASK
	c.Mutex.Unlock()
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
	<-c.stopCh

	log.Printf("The entire job has finished")

	return true
}

//
// ConstructTask construct the Task struct
//
func ConstructTask(files []string, taskClassification int32) []*Task {
	var taskList []*Task
	for index, filename := range files {
		task_temp := Task{
			TaskIdentify: strconv.Itoa(index),
		}
		task_temp.InputFile = filename

		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		fi, err := file.Stat()
		if err != nil {
			log.Fatalf("cannot retrieve %v file struct", filename)
		}

		task_temp.TaskSize = fi.Size()
		task_temp.TaskClassification = taskClassification
		taskList = append(taskList, &task_temp)
	}

	return taskList
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Setting properties of Coordinator
	c.Task_Idle = make([]*Task, 0)
	c.Task_IN_Progress = make([]*Task, 0)
	c.Task_Completed = make([]*Task, 0)
	c.NReduce = nReduce
	c.stopCh = make(chan struct{})
	c.WaitMapGroup = sync.WaitGroup{}
	c.WaitMapGroup.Add(len(files))
	// Construct New task struct
	c.Task_Idle = append(c.Task_Idle, ConstructTask(files, MAPTASK)...)

	go c.CoordinatorCompletedHandler()

	c.server()
	return &c
}
