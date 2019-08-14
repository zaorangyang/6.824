package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	wg := new(sync.WaitGroup)
	workers := make(chan string, ntasks)
	go func() {
		for worker := range registerChan {
			fmt.Printf("Schedule: %v %s\n", phase, worker)
			workers <- worker
		}
	}()

	for i := 0; i < ntasks; i++ {
		args := DoTaskArgs{
			JobName:       jobName,
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: n_other,
		}
		if phase == mapPhase {
			args.File = mapFiles[i]
		}
		wg.Add(1)
		go callWorker(workers, &args, wg)

	}
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}

func callWorker(workers chan string, args *DoTaskArgs, wg *sync.WaitGroup) {
	worker := <-workers
	for !call(worker, "Worker.DoTask", args, nil) {
		worker = <-workers
	}
	workers <- worker
	wg.Done()
}
