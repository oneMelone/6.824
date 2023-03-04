package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// 1. Register, get rNumber, and open output files
	myIdx := CallRegister()
	rNumber := CallGetRNumber()
	if rNumber <= 0 {
		panic("illegal rNumber")
	}
	outFiles := make([]*os.File, rNumber)
	for i := 0; i < rNumber; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", myIdx, i)
		outFile, err := os.Create(fileName)
		outFiles[i] = outFile
		if err != nil {
			fmt.Println("err is ", err)
			panic("open file err")
		}
	}
	outEncoders := make([]*json.Encoder, rNumber)
	for i := 0; i < rNumber; i++ {
		outEncoders[i] = json.NewEncoder(outFiles[i])
	}

	// 2. Get map tasks from Coordinator until there is none
	for {
		idx, filename := CallGetMapTask()
		if filename == "" {
			// no more map work to do
			break
		}

		// do map job
		inputFile, err := os.Open(filename)
		if err != nil {
			panic("cannot open file")
		}
		content, err := ioutil.ReadAll(inputFile)
		if err != nil {
			panic("cannot read file")
		}
		inputFile.Close()
		kva := mapf(filename, string(content))

		// write intermediate file
		for _, kv := range kva {
			targetReduce := ihash(kv.Key) % rNumber
			targetEncoder := outEncoders[targetReduce]
			err := targetEncoder.Encode(&kv)
			if err != nil {
				panic("encode err")
			}
		}

		// tell coordinator current map job is done
		CallDoneMapJob(idx)
	}

	// 3. Wait for all map job to be done
	for !CallMapReduceFence() {
		time.Sleep(time.Second)
	}

	// 4. Get reduce number, and reduce task, read in all files, sort and reduce.
	for {
		// Get a reduce task, if ther is none, return
		rIdx, hasRJob, mNum := CallGetReduceTask()
		if !hasRJob {
			break
		}

		// Read in all files related to this reduce task
		interFiles := make([]*os.File, mNum)
		for i := range interFiles {
			fileName := fmt.Sprintf("mr-%d-%d", i, rIdx)
			interFile, err := os.Open(fileName)
			if err != nil {
				fmt.Println("open interfile error:", err)
				panic("open intermediate file error")
			}
			interFiles[i] = interFile
		}
		decoders := make([]*json.Decoder, mNum)
		for i := range interFiles {
			dec := json.NewDecoder(interFiles[i])
			decoders[i] = dec
		}

		// Read all intermediate result to mem and sort
		intermediate := []KeyValue{}
		for _, dec := range decoders {
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
		}
		sort.Sort(ByKey(intermediate))

		// do reduce and write to file
		oname := fmt.Sprintf("mr-out-%d", rIdx)
		ofile, err := os.Create(oname)
		if err != nil {
			fmt.Println("create ofile err:", err)
			panic("create ofile err!")
		}

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
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}
		ofile.Close()

		// tell the cooridinator this reduce job is done
		CallDoneReduceJob(rIdx)
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallGetMapTask() (int, string) {
	reply := GetMapTaskReply{}
	ok := call("Coordinator.GetMapTask", &GetMapTaskArgs{}, &reply)
	if ok {
		return reply.Index, reply.File
	}
	panic("Call GetMapTask err!")
}

func CallGetRNumber() int {
	reply := GetRNumberReply{}
	ok := call("Coordinator.GetRNumber", &GetRNumberArgs{}, &reply)
	if ok {
		return reply.RNumber
	}
	panic("CallGetRNumber err!")
}

func CallRegister() int {
	reply := RegisterReply{}
	ok := call("Coordinator.Register", &RegisterArgs{}, &reply)
	if ok {
		return reply.WorkerIdx
	}
	panic("Register err!")
}

func CallDoneMapJob(idx int) {
	ok := call("Coordinator.DoneMapJob", &DoneMapJobArgs{Index: idx}, &DoneMapJobReply{})
	if ok {
		return
	}
	panic("Call DoneMapJob error!")
}

func CallDoneReduceJob(idx int) {
	ok := call("Coordinator.DoneReduceTask", &DoneReduceTaskArgs{Index: idx}, &DoneReduceTaskReply{})
	if ok {
		return
	}
	panic("Call DoneReduceJob error")
}

func CallMapReduceFence() bool {
	reply := MapReduceFenceReply{}
	ok := call("Coordinator.MapReduceFence", &MapReduceFenceArgs{}, &reply)
	if ok {
		return reply.MapDone
	}
	panic("Call MapReduceFence error!")
}

func CallGetReduceTask() (int, bool, int) {
	reply := GetReduceTaskReply{}
	ok := call("Coordinator.GetReduceTask", &GetReduceTaskArgs{}, &reply)
	if ok {
		return reply.Index, reply.HasJob, reply.MapWorkerNum
	}
	panic("Call GetReduceTask error!")
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
