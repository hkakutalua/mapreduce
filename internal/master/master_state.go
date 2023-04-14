package master

import (
	"log"
	"sync"

	"github.com/hkakutalua/mapreduce/internal/pkg/rpc"
)

type WorkerStatus uint8

const (
	Online  WorkerStatus = 0
	Offline WorkerStatus = 1
)

type WorkerId uint16

type Worker struct {
	Id       WorkerId
	Hostname string
	Port     uint16
	Status   WorkerStatus
}

type MapTask struct {
	Id                     uint16
	Status                 rpc.TaskStatus
	InputFileSplitLocation string
	IntermediateFiles      []rpc.IntermediateFile
	WorkerAssignedId       *WorkerId
}

type ReduceTask struct {
	Id                 uint16
	Status             rpc.TaskStatus
	PartitionNumber    uint16
	OutputFileLocation *string
	AssignedWorkerId   *WorkerId
}

type MasterState struct {
	Workers     []Worker
	MapTasks    []MapTask
	ReduceTasks []ReduceTask
	mutex       sync.Mutex
}

func (state *MasterState) ChangeWorkerOnlineStatus(workerId WorkerId, status WorkerStatus) {
	state.mutex.Lock()
	defer state.mutex.Unlock()

	for i, worker := range state.Workers {
		if worker.Id == workerId {
			worker.Status = status
			state.Workers[i] = worker
			break
		}
	}
}

func (state *MasterState) ChangeWorkerWithMapTaskToOnline(
	workerId WorkerId,
	mapTaskId uint16,
	intermediateFiles []rpc.IntermediateFile,
	mapTaskStatus rpc.TaskStatus,
) {
	state.mutex.Lock()
	defer state.mutex.Unlock()

	worker := state.Workers[workerId]
	worker.Status = Online
	state.Workers[workerId] = worker

	for i, mapTask := range state.MapTasks {
		if mapTask.Id == mapTaskId {
			if mapTask.WorkerAssignedId == nil {
				log.Printf("Skipping map task update for worker %v because "+
					"task %v has no worker assigned to", workerId, mapTask.Id)
				return
			}

			if *(mapTask.WorkerAssignedId) != workerId {
				log.Printf("Skipping map task update for worker %v because "+
					"worker %v has already been assigned for the task %v", workerId, *(mapTask.WorkerAssignedId), mapTask.Id)
				return
			}

			if mapTask.Status == rpc.Completed {
				log.Printf("Skipping map task update for worker %v because task %v is completed",
					workerId, mapTask.Id)
				return
			}

			mapTask.IntermediateFiles = intermediateFiles
			mapTask.Status = mapTaskStatus
			state.MapTasks[i] = mapTask
			return
		}
	}
}

func (state *MasterState) ChangeWorkerWithReduceTaskToOnline(
	workerId WorkerId,
	reduceTaskId uint16,
	outputFileLocation string,
	reduceTaskStatus rpc.TaskStatus,
) {
	state.mutex.Lock()
	defer state.mutex.Unlock()

	worker := state.Workers[workerId]
	worker.Status = Online
	state.Workers[workerId] = worker

	for i, reduceTask := range state.ReduceTasks {
		if reduceTask.Id == reduceTaskId {
			if reduceTask.AssignedWorkerId == nil {
				log.Printf("Skipping reduce task update for worker %v because"+
					"task %v has no worker assigned to", workerId, reduceTask.Id)
				return
			}

			if *(reduceTask.AssignedWorkerId) != workerId {
				log.Printf("Skipping reduce task update for worker %v because"+
					"worker %v has already been assigned for the task %v", workerId, *(reduceTask.AssignedWorkerId), reduceTask.Id)
				return
			}

			if reduceTask.Status == rpc.Completed {
				log.Printf("Skipping reduce task update for worker %v because task %v is completed",
					workerId, reduceTask.Id)
				return
			}

			reduceTask.OutputFileLocation = &outputFileLocation
			reduceTask.Status = reduceTaskStatus
			state.ReduceTasks[i] = reduceTask
			return
		}
	}
}

func (state *MasterState) GetReduceTaskById(reduceTaskId uint16) *ReduceTask {
	for _, reduceTask := range state.ReduceTasks {
		if reduceTask.Id == reduceTaskId {
			return &reduceTask
		}
	}

	return nil
}
