package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
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

type ByKey []KeyValue

// for sorting by key, implementing sort interface
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

func performMap(mapf func(string, string) []KeyValue, filename string, nReduce int, index int) bool {
	// will create a 2D array in which every reducer will be alloted an arrau of keyValue pairs
	kvall := make([][]KeyValue, nReduce)
	file, err := os.Open(filename)

	if err != nil {
		fmt.Fprintf(os.Stderr, "%s Worker: can not open %v\n", time.Now().String(), filename)
		return false
	}

	content, err := io.ReadAll(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s Worker: can not read %v\n", time.Now().String(), filename)
		return false
	}

	file.Close()

	map_res := mapf(filename, string(content))

	// intermediate data is genrated
	// now map result is to be mappd with nReduce number of buckets
	for _, key_value := range map_res {
		index := ihash(key_value.Key) % nReduce
		kvall[index] = append(kvall[index], key_value)
	}

	// write key-value to differnt json files

	for i, kva := range kvall {

		// making use of tempfile for atomic writes to ensure midway failures do not give half written files
		oldname := fmt.Sprintf("temp_inter_%d_%d.json", index, i)
		tempfile, err := os.OpenFile(oldname, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s Worker: can not open temp file %v\n", time.Now().String(), oldname)
			return false
		}

		enc := json.NewEncoder(tempfile)
		for _, kv := range kva {
			if err := enc.Encode(&kv); err != nil {
				fmt.Fprintf(os.Stderr, "%s Worker: can not write to temp file  %v\n", time.Now().String(), kv)
				return false
			}
		}

		tempfile.Close()
		newname := fmt.Sprintf("inter_%d_%d.json", index, i)
		if err := os.Rename(oldname, newname); err != nil {
			fmt.Fprintf(os.Stderr, "%s Worker: map can not rename temp file %v\n", time.Now().String(), oldname)
			return false
		}
		os.Remove(oldname)
	}
	return true
}

func performReduce(reducef func(string, []string) string, split int, index int) bool {
	var kva []KeyValue

	// 1. Read ALL intermediate files first
	for i := 0; i < split; i++ {
		filename := fmt.Sprintf("inter_%d_%d.json", i, index)
		file, err := os.Open(filename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Worker: cannot read %v\n", filename)
			return false
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// 2. Sort ALL collected key-value pairs
	sort.Sort(ByKey(kva))

	// 3. Prepare atomic write
	oldname := fmt.Sprintf("temp-mr-out-%d", index)
	newname := fmt.Sprintf("mr-out-%d", index)

	tempfile, err := os.Create(oldname)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Worker: cannot create temp file %v\n", oldname)
		return false
	}
	defer os.Remove(oldname)

	// 4. Group keys and reduce
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)
		fmt.Fprintf(tempfile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	// 5. Atomically commit output
	if err := os.Rename(oldname, newname); err != nil {
		fmt.Fprintf(os.Stderr, "Worker: failed to rename %v\n", oldname)
		return false
	}

	return true
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := ReqArgs{}
		reply := ReqReply{}

		if !call("Coordinator.HandleReq", &args, &reply) {
			fmt.Fprintf(os.Stderr, "%s Worker: exit", time.Now().String())
			os.Exit(0)
		}
		if reply.Kind == "none" {
			// all map and reduce tasks are done
			break

		}

		res_args := Res_args{}
		res_reply := Res_reply{}

		if reply.Kind == "map" {
			if performMap(mapf, reply.File, reply.NReduce, reply.Index) {
				fmt.Fprintf(os.Stderr, "%s Worker: map task performed successfully %s\n", time.Now().String(), reply.File)
				res_args.Kind = "map"
				res_args.Index = reply.Index
				// map has been performed on the file
				// and interm data been stored in respected file buckets as per their key hashes

				if !call("Coordinator.HandleRes", &res_args, &res_reply) {
					fmt.Fprintf(os.Stderr, "%s Worker: exit", time.Now().String())
					os.Exit(0)
				}

			} else {
				fmt.Fprintf(os.Stderr, "%s Worker: map task failed\n", time.Now().String())
			}
		} else {
			if performReduce(reducef, reply.Split, reply.Index) {
				fmt.Fprintf(os.Stderr, "%s Worker: reduce task performed successfully\n", time.Now().String())
				res_args.Kind = "reduce"
				res_args.Index = reply.Index

				if !call("Coordinator.HandleRes", &res_args, &res_reply) {
					fmt.Fprintf(os.Stderr, "%s Worker: exit", time.Now().String())
					os.Exit(0)
				}
			} else {
				fmt.Fprintf(os.Stderr, "%s Worker: reduce task failed", time.Now().String())
				os.Exit(0)
			}
		}
		time.Sleep(time.Second)
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//

// using interface{} to make the funciton accept generic argument & reply struct
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")

	// this func genreates unique Unix domain socket path for the coordinator process
	// unique to our linux user
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
