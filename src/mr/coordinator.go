package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	Waiting TaskStatus = 1
	Doing   TaskStatus = 2
	Done    TaskStatus = 3
)

const (
	maxMapTime    = 15
	maxReduceTime = 15
)

type Coordinator struct {
	mutex            sync.Mutex   // mutex for shared data
	files            []string     // map tasks, each file is a map task
	mstatus          []TaskStatus // for each file, there is a status
	doneMapJobs      int          // how many map jobs are done now
	rNumber          int          // reduce task numbers
	rstatus          []TaskStatus // reduce task status
	doneReduceJobs   int          // how many reduce jobs are done
	workerNum        int          // total number of current workers
	remainMapTime    []int        // remain map time for the map task to be done
	remainReduceTime []int        // remain reduce time for the reduce task to be done
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
			c.remainMapTime[i] = maxMapTime
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
			c.remainReduceTime[i] = maxReduceTime
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

func (c *Coordinator) ExitFence(args *ExitFenceArgs, reply *ExitFenceReply) error {
	if c.Done() {
		reply.Done = true
	}

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

	// periodically check task status
	go func() {
		for {
			c.mutex.Lock()
			for i := range c.mstatus {
				if c.mstatus[i] == Doing {
					c.remainMapTime[i] -= 3
					fmt.Printf("map job %d remain %d\n", i, c.remainMapTime[i])
					if c.remainMapTime[i] == 0 {
						fmt.Printf("map job %d need rearrangement\n", i)
						c.mstatus[i] = Waiting
					}
				}
			}
			for i := range c.rstatus {
				if c.rstatus[i] == Doing {
					c.remainReduceTime[i] -= 3
					if c.remainReduceTime[i] == 0 {
						c.rstatus[i] = Waiting
					}
				}
			}
			c.mutex.Unlock()

			time.Sleep(3 * time.Second)
		}
	}()
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
	c.remainMapTime = make([]int, len(files))
	c.remainReduceTime = make([]int, nReduce)

	c.server()
	return &c
}
