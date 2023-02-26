package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type MapTaskStatus int

const (
	Waiting MapTaskStatus = 1
	Doing   MapTaskStatus = 2
	Done    MapTaskStatus = 3
)

type Coordinator struct {
	mutex     sync.Mutex      // mutex for shared data
	files     []string        // map tasks, each file is a map task
	status    []MapTaskStatus // for each file, there is a status
	rNumber   int             // reduce task numbers
	workerNum int             // total number of current workers
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// get a map work by this function.
func (c *Coordinator) GetMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i, s := range c.status {
		if s == Waiting {
			reply.File = c.files[i]
			reply.Index = i
			return nil
		}
	}
	return nil
}

// get rNumber
func (c *Coordinator) GetRNumber(args *GetRNumberArgs, reply *GetRNumberReply) error {
	reply.RNumber = c.rNumber
	return nil
}

// worker registers and get a number
func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	idx := c.workerNum
	c.workerNum += 1
	reply.WorkerIdx = idx
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
