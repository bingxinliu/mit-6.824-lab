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


// MRTask type
const (
    MapTask = "MapTask"
    ReduceTask = "ReduceTask"
)

// descriptor for a task
type TaskState struct {
    td TaskDesc
    isFinished bool
}

type TaskDesc struct {
    Type string
    TaskId string
    StartTime time.Time
    EndTime time.Time
}

type Coordinator struct {
	// Your definitions here.
    mu sync.Mutex
    files []string
    f_idx int
    workersTable map[string]*TaskState;
    nTasks uint
    nReduce int
    mapTaskTable map[string][]*TaskState;
    reduceTaskTable map[string][]*TaskState;
    mapTasksDone bool;
    reduceTaskDone bool;
    allDone bool;
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Register(args *TaskArgs, reply *TaskReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    // c.nTasks += 1
    reply.Key = string(c.nReduce)
    return nil
}

func (c *Coordinator) GetWork(args *TaskArgs, reply *TaskReply) error {
    // lock data structure
    c.mu.Lock()
    defer c.mu.Unlock()
    // check
    if !c.mapTasksDone {
        // All Map task dispatched?
        if c.f_idx >= len(c.files) {
            // temp disable repatch
            reply.Key = ""
        } else {
            reply.Key = c.files[c.f_idx]
            c.f_idx += 1
            ts := TaskState{ TaskDesc{ MapTask, "", time.Now(), time.Time{} }, false }
            c.workersTable[args.Key] = &ts
            c.mapTaskTable[reply.Key] = append( c.mapTaskTable[reply.Key], &ts)
        }
        return nil
    }

    if !c.reduceTaskDone {
    }

    return nil
}

func (c *Coordinator) CompleteWork(args *TaskArgs, _ *TaskReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    ts, exist := c.workersTable[args.Key]
    if !exist { log.Fatal("Coordinator.CompleteWork Failed, no such worker exist");
    ts.td.EndTime = time.Now()
    ts.isFinished = true

    // check Map Tasks
    if !c.mapTasksDone {
        all_task_done := true
        for k, v := range c.mapTaskTable {
            current_task_done := false
            for _, ts := range v {
                if ts.isFinished { current_task_done = true }
            }
            if !current_task_done { 
                all_task_done = false
                break;
            }
        }
        if all_task_done { c.mapTasksDone = true }
        return nil
    }

    if !c.reduceTaskDone {
        all_task_done := true
        for k, v := range c.reduceTaskTable {
            current_task_done := false
            for _, ts := range v {
                if ts.isFinished { current_task_done = true }
            }
            if !current_task_done { 
                all_task_done = false
                break;
            }
        }
        if all_task_done {
            c.reduceTaskTable = true
            c.allDone = true
        }
        return nil
    }

    return nil
}

func (c *Coordinator) ShouldStop(_ *MapArgs, reply *MapReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    if c.allDone {
        reply.Key = "true"
    } else {
        reply.Key = "false"
    }

    return nil;
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
    // c.nWorkers = 0;
    c.nReduce = nReduce
    c.files = files
    c.f_idx = 0
    c.mapTasksDone = false;
    c.reduceTaskDone = false;
    c.allDone = false;
    c.mapTaskTable = make(map[string][]*TaskState, len(files)
    for i, s := range files {
        fmt.Println("file[", i, "](", s, ")")
        c.mapTaskTable[s] = make([]*TaskState)
    }
    for i := 0; i < nReduce; i++ {
        c.reduceTaskTable[strconv.Itoa(i)] = make([]*TaskState) 
    }


	c.server()
	return &c
}
