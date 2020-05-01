package mr

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
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

	// uncomment to send the Example RPC to the master.
	// CallExample()

	for {
		replyArgs := Request()
		switch task := replyArgs.Task; task {
		case "Map":
			// Find all the Key-Value pairs
			fmt.Printf("Map Task: %d\n", replyArgs.TaskNumber)
			intermediate := []KeyValue{}
			for i := 0; i < replyArgs.NumFiles; i++ {
				filename := replyArgs.Files[i]
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				intermediate = append(intermediate, kva...)
			}

			sort.Sort(ByKey(intermediate))
			// Now write each of the pair in the appropriate file
			for i := 0; i < replyArgs.ReduceNumber; i++ {
				file, _ := os.Create(fmt.Sprintf("%s%d%s%d", "mr-", replyArgs.TaskNumber, "-", i))
				file.Close()
			}
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}

				hash := ihash(intermediate[i].Key)
				reduceTaskNumber := hash % (replyArgs.ReduceNumber)
				oname := fmt.Sprintf("%s%d%s%d", "mr-", replyArgs.TaskNumber, "-", reduceTaskNumber)

				file, err := os.OpenFile(oname, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
				if err != nil {
					panic(err)
				}
				for k := i; k < j; k++ {
					file.WriteString(fmt.Sprintf("%v %v\n", intermediate[i].Key, intermediate[k].Value))
				}
				file.Close()
				i = j
			}
			Confirm("MapDone", replyArgs.TaskNumber)
		case "Reduce":
			intermediate := []KeyValue{}
			// fmt.Printf("Reduce Task: %d\n", replyArgs.TaskNumber)
			for i := 0; i < replyArgs.MapNumber; i++ {
				iname := fmt.Sprintf("%s%d%s%d", "mr-", i, "-", replyArgs.TaskNumber)
				// fmt.Printf("%s\n", iname)
				file, err := os.Open(iname)
				if err != nil {
					log.Fatalf("cannot open %v", iname)
				}
				content, err := ioutil.ReadFile(iname)
				if err != nil {
					log.Fatalf("cannot read %v", iname)
				}
				file.Close()
				pairs := bytes.Split(content, []byte("\n"))
				for j := 0; j < len(pairs)-1; j++ {
					values := bytes.Split(pairs[j], []byte(" "))
					intermediate = append(intermediate, KeyValue{Key: string(values[0]), Value: string(values[1])})
				}
			}

			sort.Sort(ByKey(intermediate))
			oname := fmt.Sprintf("%s%d", "mr-out-", replyArgs.TaskNumber)
			ofile, _ := os.Create(oname)
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
			Confirm("ReduceDone", replyArgs.TaskNumber)
		default:
		}
	}
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	// fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// sends a request to master server asking for a task
//
func Request() ReplyArgs {

	args := RequestArgs{}

	args.State = "Ready"

	reply := ReplyArgs{}

	call("Master.RequestHandle", &args, &reply)

	return reply

}

//
// confirms
//
func Confirm(conf string, num int) ReplyArgs {

	args := RequestArgs{}

	args.State = conf
	args.Num = num
	fmt.Printf("Confirm: %s %d\n", conf, num)
	reply := ReplyArgs{}

	call("Master.RequestHandle", &args, &reply)

	return reply

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
