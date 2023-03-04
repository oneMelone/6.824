package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type TaskStatus int

const (
	Waiting TaskStatus = 1
	Doing   TaskStatus = 2
	Done    TaskStatus = 3
)

type Coordinator struct {
	mutex          sync.Mutex   // mutex for shared data
	files          []string     // map tasks, each file is a map task
	mstatus        []TaskStatus // for each file, there is a status
	doneMapJobs    int          // how many map jobs are done now
	rNumber        int          // reduce task numbers
	rstatus        []TaskStatus // reduce task status
	doneReduceJobs int          // how many reduce jobs are done
	workerNum      int          // total number of current workers
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

	for i, s := range c.mstatus {
		if s == Waiting {
			reply.File = c.files[i]
			reply.Index = i
			c.mstatus[i] = Doing
			return nil
		}
	}
	return nil
}

// get a reduce work by this function
func (c *Coordinator) GetReduceTask(args *GetReduceTaskArgs, reply *GetReduceTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i, s := range c.rstatus {
		if s == Waiting {
			reply.Index = i
			reply.HasJob = true
			reply.MapWorkerNum = c.workerNum
			c.rstatus[i] = Doing
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

func (c *Coordinator) DoneMapJob(args *DoneMapJobArgs, reply *DoneMapJobReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.Index < 0 || args.Index >= len(c.mstatus) {
		return errors.New("wrong index")
	}
	if c.mstatus[args.Index] == Doing {
		c.doneMapJobs += 1
	}
	c.mstatus[args.Index] = Done
	return nil
}

func (c *Coordinator) DoneReduceTask(args *DoneReduceTaskArgs, reply *DoneReduceTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.Index < 0 || args.Index >= len(c.rstatus) {
		return errors.New("wrong rindex")
	}
	if c.rstatus[args.Index] == Doing {
		c.doneReduceJobs += 1
	}
	c.rstatus[args.Index] = Done
	return nil
}

func (c *Coordinator) MapReduceFence(args *MapReduceFenceArgs, reply *MapReduceFenceReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.doneMapJobs == len(c.files) {
		reply.MapDone = true
		return nil
	}
	reply.MapDone = false
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

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.doneReduceJobs == c.rNumber {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.mutex = sync.Mutex{}
	c.files = files
	c.mstatus = make([]TaskStatus, len(files))
	for i := range c.mstatus {
		c.mstatus[i] = Waiting
	}
	c.doneMapJobs = 0
	c.rNumber = nReduce
	c.rstatus = make([]TaskStatus, nReduce)
	for i := range c.rstatus {
		c.rstatus[i] = Waiting
	}
	c.doneReduceJobs = 0
	c.workerNum = 0

	c.server()
	return &c
}
