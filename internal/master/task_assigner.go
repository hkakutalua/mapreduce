package master

import (
	"fmt"
	"log"

	"github.com/hkakutalua/mapreduce/internal/pkg/rpc"
)

func AssignIdleMapTasksToAvailableWorkers(masterState *MasterState, rpcGateway rpc.RpcGateway) {
	masterState.mutex.Lock()
	defer masterState.mutex.Unlock()

	availableWorkers := getAvailableWorkersForMap(masterState.Workers, masterState.MapTasks)
	idleMapTasks := getIdleMapTasks(masterState.MapTasks)
	assignedWorkers := make([]Worker, 0)
	assignedMapTasks := make([]MapTask, 0)

	if len(availableWorkers) != 0 && len(idleMapTasks) != 0 {
		for _, worker := range availableWorkers {
			for _, mapTask := range idleMapTasks {
				if len(assignedWorkers) == len(availableWorkers) ||
					len(assignedMapTasks) == len(idleMapTasks) {
					return
				}

				if wasMapTaskPreviouslyAssigned(mapTask, assignedMapTasks) {
					continue
				}

				workerAddress := fmt.Sprintf("%v:%v", worker.Hostname, worker.Port)
				args := rpc.StartMapTaskArgs{Id: mapTask.Id, InputFileSplitLocation: mapTask.InputFileSplitLocation}
				err := rpcGateway.Call(workerAddress, "WorkerServer.StartMapTask", args, &rpc.StartMapTaskReply{})

				if err != nil {
					log.Printf("could not assign map task %v to available worker %v: %v", mapTask.Id, worker.Id, err)
					break
				}

				masterState.AssignMapTaskToWorker(mapTask.Id, worker.Id)

				assignedWorkers = append(assignedWorkers, worker)
				assignedMapTasks = append(assignedMapTasks, mapTask)

				break
			}
		}
	}
}

func AssignIdleReduceTasksToAvailableWorkers(masterState *MasterState, rpcGateway rpc.RpcGateway) {
	if !allMapTasksAreCompleted(masterState.MapTasks) {
		log.Printf("not all map tasks are completed, ignoring reduce task assignment")
		return
	}

	availableWorkers := getAvailableWorkersForReduce(masterState.Workers, masterState.ReduceTasks)
	idleReduceTasks := getIdleReduceTasks(masterState.ReduceTasks)
	intermediateFilesGroupedByPartition := getIntermediateFilesGroupedByPartition(masterState.MapTasks)

	for _, worker := range availableWorkers {
		for _, reduceTask := range idleReduceTasks {
			if masterState.ReduceTasks[reduceTask.Id].WorkerAssignedId != nil {
				continue
			}

			workerAddress := fmt.Sprintf("%v:%v", worker.Hostname, worker.Port)
			args := rpc.StartReduceTaskArgs{
				Id:                reduceTask.Id,
				IntermediateFiles: intermediateFilesGroupedByPartition[reduceTask.PartitionNumber],
			}
			err := rpcGateway.Call(workerAddress, "WorkerServer.StartReduceTask", args, &rpc.StartReduceTaskReply{})
			if err != nil {
				log.Printf("could not assign reduce task %v to available worker %v: %v", reduceTask.Id, worker.Id, err)
				break
			}

			masterState.AssignReduceTaskToWorker(reduceTask.Id, worker.Id)

			break
		}
	}
}

func allMapTasksAreCompleted(mapTasks []MapTask) bool {
	for _, mapTask := range mapTasks {
		if mapTask.Status != rpc.Completed {
			return false
		}
	}

	return true
}

func wasMapTaskPreviouslyAssigned(mapTask MapTask, assignedMapTasks []MapTask) bool {
	for _, assignedMapTask := range assignedMapTasks {
		if assignedMapTask.Id == mapTask.Id {
			return true
		}
	}

	return false
}

func getAvailableWorkersForMap(workers []Worker, mapTasks []MapTask) []Worker {
	availableWorkers := make([]Worker, 0)

	for _, worker := range workers {
		hasWorkerAnAssignedInProgressMapTask := false
		for _, mapTask := range mapTasks {
			if mapTask.WorkerAssignedId != nil &&
				*mapTask.WorkerAssignedId == worker.Id &&
				mapTask.Status == rpc.InProgress {
				hasWorkerAnAssignedInProgressMapTask = true
				break
			}
		}

		if worker.Status == Online && !hasWorkerAnAssignedInProgressMapTask {
			availableWorkers = append(availableWorkers, worker)
		}
	}

	return availableWorkers
}

func getAvailableWorkersForReduce(workers []Worker, reduceTasks []ReduceTask) []Worker {
	availableWorkers := make([]Worker, 0)

	for _, worker := range workers {
		hasWorkerAnAssignedInProgressReduceTask := false
		for _, reduceTask := range reduceTasks {
			if reduceTask.WorkerAssignedId != nil &&
				*reduceTask.WorkerAssignedId == worker.Id &&
				reduceTask.Status == rpc.InProgress {
				hasWorkerAnAssignedInProgressReduceTask = true
				break
			}
		}

		if worker.Status == Online && !hasWorkerAnAssignedInProgressReduceTask {
			availableWorkers = append(availableWorkers, worker)
		}
	}

	return availableWorkers
}

func getIdleMapTasks(mapTasks []MapTask) []MapTask {
	idleMapTasks := make([]MapTask, 0)

	for _, mapTask := range mapTasks {
		if mapTask.Status == rpc.Idle {
			idleMapTasks = append(idleMapTasks, mapTask)
		}
	}

	return idleMapTasks
}

func getIdleReduceTasks(reduceTasks []ReduceTask) []ReduceTask {
	idleReduceTasks := make([]ReduceTask, 0)

	for _, reduceTask := range reduceTasks {
		if reduceTask.Status == rpc.Idle {
			idleReduceTasks = append(idleReduceTasks, reduceTask)
		}
	}

	return idleReduceTasks
}

func getIntermediateFilesGroupedByPartition(mapTasks []MapTask) map[uint16][]rpc.IntermediateFile {
	intermediateFilesByPartition := make(map[uint16][]rpc.IntermediateFile)

	for _, mapTask := range mapTasks {
		if mapTask.Status == rpc.Completed {
			for _, intermediateFile := range mapTask.IntermediateFiles {
				if intermediateFilesByPartition[intermediateFile.Partition] == nil {
					intermediateFilesByPartition[intermediateFile.Partition] = []rpc.IntermediateFile{}
				}

				intermediateFilesByPartition[intermediateFile.Partition] = append(
					intermediateFilesByPartition[intermediateFile.Partition],
					intermediateFile,
				)
			}
		}
	}

	return intermediateFilesByPartition
}
