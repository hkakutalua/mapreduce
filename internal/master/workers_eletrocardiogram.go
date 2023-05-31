package master

import (
	"log"
	"strconv"

	"github.com/hkakutalua/mapreduce/internal/pkg/rpc"
)

func UpdateWorkerTasksFromHeartbeats(state *MasterState, rpcGateway rpc.RpcGateway) {
	state.mutex.Lock()
	defer state.mutex.Unlock()

	for _, worker := range state.Workers {
		workerAddress := worker.Hostname + ":" + strconv.Itoa(int(worker.Port))
		var heartBeatReply *rpc.GetHeartBeatReply = nil

		err := rpcGateway.Call(workerAddress, "WorkerServer.GetHeartbeat", rpc.GetHeartBeatArgs{}, &heartBeatReply)

		if err != nil {
			state.ChangeWorkerOnlineStatus(worker.Id, Offline)
			continue
		}

		if heartBeatReply.MapTask == nil && heartBeatReply.ReduceTask == nil {
			state.ChangeWorkerOnlineStatus(worker.Id, Online)
			continue
		}

		if heartBeatReply.MapTask != nil {
			state.ChangeWorkerWithMapTaskToOnline(
				worker.Id,
				heartBeatReply.MapTask.Id,
				heartBeatReply.MapTask.IntermediateFiles,
				heartBeatReply.MapTask.Status,
			)
		}

		if heartBeatReply.ReduceTask != nil {
			state.ChangeWorkerOnlineStatus(worker.Id, Online)

			reduceTask := state.GetReduceTaskById(heartBeatReply.ReduceTask.Id)
			if reduceTask == nil {
				log.Panicf("reduce task %v not found", heartBeatReply.ReduceTask.Id)
			}

			if *reduceTask.WorkerAssignedId != worker.Id && heartBeatReply.ReduceTask.Status == rpc.InProgress {
				log.Printf(
					"a different worker (Id %v) is already assigned for the reduce task (Id %v) in progress in worker %v.",
					*reduceTask.WorkerAssignedId, reduceTask.Id, worker.Id,
				)
				log.Printf("aborting duplicated reduce task %v on worker %v.", reduceTask.Id, worker.Id)

				err := rpcGateway.Call(
					workerAddress, "WorkerServer.AbortReduceTask", rpc.AbortReduceTaskArgs{}, &rpc.AbortReduceTaskReply{},
				)
				if err != nil {
					log.Printf("could not abort reduce task %v on worker %v: %v", reduceTask.Id, worker.Id, err)
				}

				continue
			}

			state.ChangeWorkerWithReduceTaskToOnline(
				worker.Id,
				heartBeatReply.ReduceTask.Id,
				heartBeatReply.ReduceTask.OutputFileLocation,
				heartBeatReply.ReduceTask.Status,
			)
		}
	}
}
