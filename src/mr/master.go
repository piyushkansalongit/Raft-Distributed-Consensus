package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

//
// For storing the state of tasks sent out
//
type WorkerState struct {
	sentAt      time.Time
	confimation bool
}

//
//For storing the state of master
//
type Master struct {
	// Your definitions here.
	files []string

	currentReduceTask int
	ReduceTasksDone   int
	nReduce           int
	reduceDone        []WorkerState

	currentMapTask int
	mapTasksDone   int
	numMapTasks    int
	mapDone        []WorkerState
	mu             sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// handles the request for the task from the worker
//
func (m *Master) RequestHandle(args *RequestArgs, reply *ReplyArgs) error {
	switch state := args.State; state {
	case "Ready":
		m.mu.Lock()
		if m.currentMapTask < m.numMapTasks {
			reply.Task = "Map"
			reply.NumFiles = 1
			reply.TaskNumber = m.currentMapTask
			workerState := WorkerState{sentAt: time.Now(), confimation: false}
			m.mapDone = append(m.mapDone, workerState)
			reply.ReduceNumber = m.nReduce
			var returnFiles []string
			returnFiles = append(returnFiles, m.files[m.currentMapTask])
			reply.Files = returnFiles
			m.currentMapTask++
		} else if m.currentMapTask == m.numMapTasks && m.mapTasksDone != m.numMapTasks {
			for i := 0; i < m.numMapTasks; i++ {
				diff := time.Now().Sub(m.mapDone[i].sentAt)
				if m.mapDone[i].confimation == false && diff.Seconds() >= 10 {
					fmt.Printf("Retrying task %d\n", i)
					fmt.Printf("%d\n", i)
					reply.Task = "Map"
					reply.NumFiles = 1
					reply.TaskNumber = i
					m.mapDone[i].sentAt = time.Now()
					reply.ReduceNumber = m.nReduce
					var returnFiles []string
					returnFiles = append(returnFiles, m.files[i])
					reply.Files = returnFiles
					break
				}
			}
		} else if m.mapTasksDone == m.numMapTasks && m.currentReduceTask < m.nReduce {
			reply.Task = "Reduce"
			reply.TaskNumber = m.currentReduceTask
			reply.MapNumber = m.numMapTasks
			workerState := WorkerState{sentAt: time.Now(), confimation: false}
			m.reduceDone = append(m.reduceDone, workerState)
			m.currentReduceTask++
		} else if m.currentReduceTask == m.nReduce && m.ReduceTasksDone != m.nReduce {
			for i := 0; i < m.nReduce; i++ {
				diff := time.Now().Sub(m.reduceDone[i].sentAt)
				if m.reduceDone[i].confimation == false && diff.Seconds() >= 10 {
					reply.Task = "Reduce"
					reply.TaskNumber = i
					reply.MapNumber = m.numMapTasks
					m.reduceDone[i].sentAt = time.Now()
				}
			}
		}
		m.mu.Unlock()
	case "MapDone":
		m.mapTasksDone++
		m.mu.Lock()
		m.mapDone[args.Num].confimation = true
		fmt.Printf("Map Task %d confirmed\n", args.Num)
		m.mu.Unlock()

	case "ReduceDone":
		m.ReduceTasksDone++
		m.mu.Lock()
		m.reduceDone[args.Num].confimation = true
		fmt.Printf("Reduce Task %d confirmed and completed %d\n", args.Num, m.ReduceTasksDone)
		m.mu.Unlock()
	default:
		fmt.Printf("%s.\n", state)
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	m.mu.Lock()
	rpc.Register(m)
	m.mu.Unlock()
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.mu.Lock()
	if m.ReduceTasksDone == m.nReduce {
		ret = true
	}
	m.mu.Unlock()

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{files: files, nReduce: nReduce, currentMapTask: 0, numMapTasks: len(files)}

	// Your code here.

	m.server()
	return &m
}
