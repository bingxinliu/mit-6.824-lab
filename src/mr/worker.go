package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
    var id string
    var nReduce int = 0
    // call to get map work
    args    := MapArgs{}
    reply   := MapReply{}

    args.Key = string(os.Getpid())

    // register self
    ok := call("Coordinator.Register", &args, &reply)
    if ok {
        var err error
        nReduce, err = strconv.Atoi(reply.Key)
        if err != nil { log.Fatal(err) }
    } else {
        fmt.Println("Error: can not get ID")
    }

    // get jod till should stop
    for {
        ok = call("Coordinator.GetWork", &args, &reply)
        if !ok { fmt.Println("No Map work or Get map work failed") }
            
        // test
        fmt.Println("Map work for file:", reply.Key)
        filename := reply.Key
        if reply.Key != "" {
            contents, err := os.ReadFile(filename)
            if err != nil { log.Fatal(err) }
            kv := mapf(reply.Key, string(contents))

            outfile, err := os.Create(fmt.Sprintf("mr-worker-%s", id))
            if err != nil { log.Fatal(err) }

            enc := json.NewEncoder(outfile)
            for _, v := range kv {
                err := enc.Encode(v)
                if err != nil { log.Fatal(err) }
            }
        }

        // tell coordinator we have finished
        ok = call("Coordinator.CompleteWork", &args, &reply)
        if !ok { log.Fatal("Coordinator.CompleteWork Failed") }
            
        ok = call("Coordinator.ShouldStop", &args, &reply)
        if !ok { fmt.Println("Worker RPC Coordinator.ShouldStop failed") }

        if reply.Key == "true" { break }

    }
    // At this point nothing can do, return.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}
            // encoding in json
            // m := make(map[int]*json.Encoder)
            // for _, v := range kv {
                // // fmt.Println("[", v.Key, ",", v.Value, "]")
                // bucknum := ihash(v.Key) % 10
                // if _, ok := m[bucknum]; !ok {
                    // outfile, err := os.Create(fmt.Sprintf("mr-out-%d", bucknum))
                    // if err != nil { log.Fatal(err) }
                    // m[bucknum] = json.NewEncoder(outfile)
                // }
                // // err := enc.Encode(v)
                // err := m[bucknum].Encode(v)
                // if err != nil { log.Fatal(err) }
            // }

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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
