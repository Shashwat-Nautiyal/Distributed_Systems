The template for this repo is forked from `git://g.csail.mit.edu/6.5840-golabs-2025`, but the implementation is mine. <br>
The repo will contain all the lab assingments for MIT-6.824: Distributed Systems. 


# Lab 1: MapReduce

## Overview

A Go implementation of MapReduce, a programming model for processing large datasets across distributed systems. MapReduce simplifies distributed data processing by abstracting the complexities of parallelization, fault-tolerance, data distribution, and load balancing behind a clean programming interface.<br>
This implementation is designed to understand the fundamentals of distributed computing, fault tolerance, and parallel processing.<br>
This project was made using [this](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) paper as reference.,br>
My implementation passes all the test cases except for the early-exit, and so far I haven't been able to pinpoint why exactly it is failing.

## Architecture

The MapReduce system consists of two main components:
### Master (Coordinator)
- Coordinates the entire MapReduce operation
- Distributes map and reduce tasks to workers
- Monitors worker health and handles failures
- Manages intermediate files and task scheduling
- Maintains state information about task progress

### Workers
- Infitely requests the coordinator for tasks, until the coordinator exits.
- Executes map and reduce tasks as assigned by the master
- Handles input/output operations for their assigned tasks
- Communicates with master about task completion and status
- Can be dynamically added or removed from the cluster

## Relevant Directories

```
src/
├── main/           # Main applications and test programs (provided in template )
├── mrapps/         # MapReduce applications (map and reduce functions) (provided in template )
└── mr/             # Core MapReduce library implementation (implemented by me)
```

### Map Phase
1. Already partitioned input is distributed to map workers
2. Each map worker applies the map function to its input chunk
3. Map function outputs key-value pairs
4. Output is partitioned into nReduce buckets using hash function
5. Intermediate files are written to disk `inter_{map-task-id}_{bucket-id}`

### Reduce Phase
1. Reduce workers read intermediate files from all map tasks
2. Data is sorted by key to group related values
3. Reduce function is applied to each unique key and its values
4. Final output is written to result files `mr-out-{bucket-id}`

### Coordination
1. Master assigns tasks to available workers
2. Workers report task completion back to master
3. Master tracks task states and handles failures
4. Master detects failed workers through timeouts and automatically reassigns them to healthy workers
5. System proceeds to reduce phase after all map tasks complete

## Specific Design Choices

| **Unix Domain Socket** | **TCP Socket** |
|-------------------------|----------------|
| Communicates via a file on disk | Communicates via an IP/port |
| Only works for processes on same host | Can work across machines |
| Lower overhead, faster for local IPC | Slightly more overhead, network stack involved |
| Access controlled by filesystem permissions | Access controlled by firewall, IP, etc. |

## References

- [MapReduce: Simplified Data Processing on Large Clusters](https://research.google/pubs/mapreduce-simplified-data-processing-on-large-clusters/) - Original Google paper
- [MIT 6.824 Distributed Systems](https://pdos.csail.mit.edu/6.824/) - Course materials and assignments
- [Go RPC Package Documentation](https://golang.org/pkg/net/rpc/) - RPC implementation details

---
