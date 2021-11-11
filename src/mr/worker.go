package mr

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/big"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

const SINGELETASK int = 1

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

var completedTaskCh chan *Task
var worker_name string

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
// TaskCompletedHandler Sends Completed task to the
// Coordinator and ensure the coordinator receives the
// message correctly
//
func TaskCompletedHandler(completedTaskCh chan *Task) {
	for completedTask := range completedTaskCh {
		request := Request{
			Name:          worker_name,
			CompletedTask: completedTask,
		}
		response := Response{}
		call("Coordinator.TaskCompletedSignalHandler", &request, &response)
		if response.Message == SUCCESSFUL_RECEIVE {
			log.Printf("Coordinator Successfully receives task completed message")
		} else {
			log.Printf("Completed Task announcement Missing")
		}
	}
}

//
// Worker do the main logic of Tasks
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	completedTaskCh = make(chan *Task, 5)
	go TaskCompletedHandler(completedTaskCh)
	randInt, _ := rand.Int(rand.Reader, big.NewInt(1<<32))
	worker_name = fmt.Sprintf("%x", sha256.Sum256(randInt.Bytes()))
	for {
		time.Sleep(time.Second)
		resp := RetrieveTask(worker_name)
		if resp.Message == NORMAL && resp.AssignedTask.TaskClassification == MAPTASK {
			RunMapTask(resp.AssignedTask, mapf, resp.NReduce)
			completedTaskCh <- resp.AssignedTask
		}
		if resp.Message == TASKSETS {
			RunReduceTask(resp.TaskSet, reducef, resp.NReduce)
			for _, task := range resp.TaskSet {
				completedTaskCh <- task
			}
		}
	}
}

//
// RunReduceTask actually do the ReduceTask logic
//
func RunReduceTask(assignedTask []*Task, reducef func(string, []string) string, nreduce int) {
	intermediate := []KeyValue{}
	outfilename := "mr-out-" + string(assignedTask[0].TaskIdentify)
	log.Printf("Receive ReduceTask %v ", assignedTask[0].TaskIdentify)

	for _, task := range assignedTask {
		task.OutputFile = outfilename
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
	log.Printf("outfilename %v", assignedTask[0].OutputFile)
	ofile, err := os.OpenFile(assignedTask[0].OutputFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("cannot create/open outputfile for reducetask %v", assignedTask[0].OutputFile)
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
	log.Printf("ReduceTask completed %v", assignedTask[0].TaskIdentify)
}

//
// RunMapTask perform the MapTask logic
//
func RunMapTask(assignedTask *Task, mapf func(string, string) []KeyValue, nreduce int) {
	log.Printf("Receive MapTask %v ", assignedTask.TaskIdentify)
	intermediate := []KeyValue{}

	// open input file that was assigned by the coordinator
	file, err := os.Open(assignedTask.InputFile)
	if err != nil {
		log.Fatalf("cannot open file %v", assignedTask.TaskIdentify)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", assignedTask.TaskIdentify)
	}
	defer file.Close()

	kva := mapf(assignedTask.InputFile, string(content))
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))

	i := 0
	var output_filenameSet = make(map[string]int)
	for i < len(intermediate) {
		j := i + 1
		reduceTaskID := ihash(intermediate[i].Key) % nreduce
		output_filename := "mr-" + assignedTask.TaskIdentify + "-" + strconv.Itoa(reduceTaskID)
		output_filenameSet[output_filename] = reduceTaskID
		outputfile, err := os.OpenFile(output_filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatalf("cannot open/create intermediate file for task %v", assignedTask.TaskIdentify)
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

	for filename, reduceID := range output_filenameSet {
		reduceTask := Task{
			TaskIdentify:       strconv.Itoa(reduceID),
			TaskClassification: REDUCETASK,
			InputFile:          filename,
			OutputFile:         "",
		}
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v file", filename)
		}
		fi, err := file.Stat()
		if err != nil {
			log.Fatalf("cannot retrieve file struct %v", filename)
		}
		reduceTask.TaskSize = fi.Size()
		AddingReduceTask(&reduceTask)
	}
	log.Printf("Intermediate file create for task %v", assignedTask.TaskIdentify)
}

//
// RetrieveTask retrieve the task from the coordinator
//
func RetrieveTask(worker_name string) Response {
	request := Request{Name: worker_name}

	response := Response{}
	call("Coordinator.AssignTasks", &request, &response)
	return response
}

func AddingReduceTask(reduceTask *Task) {
	request := Request{
		Name:          worker_name,
		CompletedTask: reduceTask,
	}
	response := Response{}
	call("Coordinator.AddReduceTask", &request, &response)
	if response.Message == SUCCESSFUL_ADDINGTASK {
		log.Printf("Adding ReduceTask %v success", reduceTask.InputFile)
	}
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
