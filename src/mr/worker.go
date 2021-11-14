package mr

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

var completedCh chan *TaskGroup = make(chan *TaskGroup)
var WorkerInfo WorkerProperties = WorkerProperties{WorkerName: getWorkerName()}

type WorkerProperties struct {
	WorkerName string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func getWorkerName() string {
	randBytes := make([]byte, 4)
	rand.Read(randBytes)
	return fmt.Sprintf("%x", randBytes)
}

//
// brokenFixed remove the broken files
//
func brokenFixed(taskType int32, taskID int32, NReduce int) {

	var fileName string
	switch taskType {
	case MAPTASK:
		for i := 0; i < NReduce; i++ {
			fileName = "mr-" + strconv.Itoa(int(taskID)) + "-" + strconv.Itoa(i)
			os.Remove(fileName)

		}
	case REDUCETASK:
		fileName = "mr-out" + strconv.Itoa(int(taskID))
		os.Remove(fileName)
	}
}

func completedMessageSender() {
	for {
		completedTask := <-completedCh
		request := Request{
			WorkerInfo:    WorkerInfo,
			CompletedTask: completedTask,
		}
		response := Response{}
		call("Coordinator.TaskCompletedHandler", &request, &response)
		if response.MessageType != COORDINATOR_RECEIVE {
			log.Fatalf("Loss Connection to Coordinator")
		}
		log.Printf("TaskID %v has completed", completedTask.TaskID)
	}
}

//
// Worker do the main logic of Tasks
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	go completedMessageSender()

	for {
		time.Sleep(time.Second)
		request := Request{
			WorkerInfo: WorkerInfo,
		}
		response := Response{
			MessageType: TASKASSKING,
		}
		call("Coordinator.AssignedTask", &request, &response)
		if response.MessageType == NO_TASK_TO_ASSIGN {
			continue
		}
		// Crash Recovery
		brokenFixed(response.AssignedTask.TaskType, response.AssignedTask.TaskID, response.NReduce)
		retrieveTask := response.AssignedTask

		switch retrieveTask.TaskType {
		case MAPTASK:
			{
				RunMapTask(retrieveTask, mapf, response.NReduce)
				completedCh <- retrieveTask
			}
		case REDUCETASK:
			RunReduceTask(retrieveTask, reducef, response.NReduce)
			completedCh <- retrieveTask
		}

	}
}

func RunMapTask(assignedTask *TaskGroup, mapf func(string, string) []KeyValue, nreduce int) error {
	log.Printf("Receive MapTask %v ", assignedTask.TaskID)
	implementTask := assignedTask.TaskList[0]
	intermediate := []KeyValue{}

	// open input file that was assigned by the coordinator
	file, err := os.Open(implementTask.InputFile)
	if err != nil {
		log.Fatalf("cannot open file %v", implementTask.InputFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", implementTask.InputFile)
	}
	defer file.Close()

	// call user's MapTask to finish the task
	kva := mapf(implementTask.InputFile, string(content))
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))

	// creating intermediate files and spliting inputfile based on key hash
	i := 0
	var output_filenameSet = make(map[string]int)
	for i < len(intermediate) {
		j := i + 1
		reduceTaskID := ihash(intermediate[i].Key) % nreduce
		output_filename := "mr-" + strconv.Itoa(int(assignedTask.TaskID)) + "-" + strconv.Itoa(reduceTaskID)
		output_filenameSet[output_filename] = reduceTaskID
		outputfile, err := os.OpenFile(output_filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatalf("cannot open/create intermediate file for task %v", output_filename)
		}
		enc := json.NewEncoder(outputfile)
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			enc.Encode(&intermediate[j])
			j++
		}
		enc.Encode(&intermediate[i])
		outputfile.Close()
		i = j
	}

	log.Printf("MapTask ID %v has completed", assignedTask.TaskID)
	return nil
}

func RunReduceTask(assignedTask *TaskGroup, reducef func(string, []string) string, nreduce int) {
	intermediate := []KeyValue{}
	outfilename := "mr-out-" + strconv.Itoa(int(assignedTask.TaskID))
	log.Printf("Receive ReduceTask %v ", assignedTask.TaskID)

	for _, task := range assignedTask.TaskList {
		file, err := os.Open(task.InputFile)
		if err != nil {
			log.Printf("cannot open %v", task.InputFile)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	// create reduceTask outputfile
	log.Printf("outfilename %v", outfilename)
	ofile, err := os.OpenFile(outfilename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("cannot create/open outputfile for reducetask %v", outfilename)
	}
	defer ofile.Close()

	// implement reduceTask
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	log.Printf("ReduceTask completed %v", outfilename)
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
