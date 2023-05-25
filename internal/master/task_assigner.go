package master

import (
	"fmt"
	"log"

	"github.com/hkakutalua/mapreduce/internal/pkg/rpc"
)

func AssignIdleMapTasksToAvailableWorkers(masterState *MasterState, rpcGateway rpc.RpcGateway) {
	availableWorkers := getAvailableWorkers(masterState.Workers, masterState.MapTasks)
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

}

func wasMapTaskPreviouslyAssigned(mapTask MapTask, assignedMapTasks []MapTask) bool {
	for _, assignedMapTask := range assignedMapTasks {
		if assignedMapTask.Id == mapTask.Id {
			return true
		}
	}

	return false
}

func getAvailableWorkers(workers []Worker, mapTasks []MapTask) []Worker {
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

func getIdleMapTasks(mapTasks []MapTask) []MapTask {
	idleMapTasks := make([]MapTask, 0)

	for _, mapTask := range mapTasks {
		if mapTask.Status == rpc.Idle {
			idleMapTasks = append(idleMapTasks, mapTask)
		}
	}

	return idleMapTasks
}
