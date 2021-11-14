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
	// constant for MessageType
	TASK_ASSIGN = 1 << iota
	COMPLETEDTASK
	COORDINATOR_RECEIVE
	NO_TASK_TO_ASSIGN
	TASKASSKING
)

type Request struct {
	WorkerInfo    WorkerProperties
	CompletedTask *TaskGroup
}

type Response struct {
	MessageType  int32
	AssignedTask *TaskGroup
	NReduce      int
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
