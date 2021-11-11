package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (

	//  relevant to response message
	NO_RESOURCES          = 1
	NORMAL                = 1 << 1
	SUCCESSFUL_RECEIVE    = 1 << 2
	NICE_EXIT             = 1 << 3
	SUCCESSFUL_ADDINGTASK = 1 << 4
	TASKSETS              = 1 << 5
)

type Request struct {
	Name          string // Name of the Worker
	CompletedTask *Task
	ReduceSlice   int
}

type Response struct {
	AssignedTask *Task
	Message      int32
	NReduce      int
	TaskSet      []*Task
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
