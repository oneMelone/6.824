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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type GetMapTaskArgs struct {
}

type GetMapTaskReply struct {
	File  string // if empty, then no more work to do
	Index int    // idx for the map task
}

type GetReduceTaskArgs struct {
}

type GetReduceTaskReply struct {
	Index  int  // idx for the reduce task
	HasJob bool // if still has a reduce job
	MapNum int  // map tasks total number
}

type GetRNumberArgs struct {
}

type GetRNumberReply struct {
	RNumber int // RNumber defined by user
}

type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerIdx int
}

type DoneMapJobArgs struct {
	Index int
}

type DoneMapJobReply struct {
}

type MapReduceFenceArgs struct {
}

type MapReduceFenceReply struct {
	MapDone bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
