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

const (
	IDLE        = 0
	IN_PROGRESS = 1
	COMPLETED   = 2
	MAP         = 0
	REDUCE      = 1
)

type Coordinator struct {
	// Your definitions here.
	mut      sync.Mutex
	M_remain int
	R_remain int
	M_tasks  []*Task
	R_tasks  []*Task
}

type Task struct {
	Lock      sync.Mutex
	Filename  string
	Status    int
	Timestamp time.Time
}

func monitor(task *Task) {
	time.Sleep(10 * time.Second)
	task.Lock.Lock()

	if task.Status == COMPLETED {
		fmt.Fprintf(os.Stderr, "%s Master: task %s completed\n", time.Now().String(), task.Filename)
	} else {
		task.Status = IDLE
		fmt.Fprint(os.Stderr, "%s Master: task %s failed, re-allocate to other workers\n", time.Now().String(), task.Filename)
	}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) HandleReq(args *ReqArgs, reply *ReqReply) error {
	reply.Kind = "none"

	// looking for map task
	if c.M_remain != 0 {
		for i, task := range c.M_tasks {
			task.Lock.Lock()
			defer task.Lock.Unlock()

			if task.Status == IDLE {
				task.Status = IN_PROGRESS
				reply.Kind = "map"
				reply.File = task.Filename
				reply.NReduce = len(c.R_tasks)
				reply.Index = i
				task.Timestamp = time.Now() // that we assigned the task at this moment of time

				go monitor(task)
				break
			}
		}
	} else {
		// looking for reduce task

		for i, task := range c.R_tasks {
			task.Lock.Lock()
			defer task.Lock.Unlock()

			if task.Status == IDLE {
				task.Status = IN_PROGRESS
				task.Timestamp = time.Now()
				reply.Kind = "reduce"
				reply.Index = i
				reply.Split = len(c.M_tasks)

				go monitor(task)
				break

			}
		}
	}

	return nil
}

// func (c *Coordinator) Handle

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
	c.M_remain = len(files)
	c.R_remain = nReduce
	c.mut = sync.Mutex{}
	c.M_tasks = make([]*Task, len(files))
	c.R_tasks = make([]*Task, nReduce)

	// Your code here.

	for i, filename := range files {
		c.M_tasks[i] = new(Task)
		c.M_tasks[i].Filename = filename
		c.M_tasks[i].Status = IDLE
		c.M_tasks[i].Lock = sync.Mutex{}
	}

	for i := 0; i < nReduce; i++ {
		c.R_tasks[i] = new(Task)
		c.R_tasks[i].Status = IDLE
		c.R_tasks[i].Lock = sync.Mutex{}
	}

	fmt.Fprintf(os.Stderr, "%s Master: initialization completed\n", time.Now().String())

	c.server()
	return &c
}
