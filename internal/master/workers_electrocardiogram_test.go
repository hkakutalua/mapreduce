package master

import (
	"fmt"
	"testing"

	"github.com/hkakutalua/mapreduce/internal/pkg/rpc"
	"github.com/stretchr/testify/assert"
)

func TestThat_ItShouldUpdateWorkerToOnline_WhenWorkerIsOnlineOnScanning(t *testing.T) {
	mockWorkerServer := WorkerServer{}
	mockWorkerServer.EnqueueGetHeartbeatReply(rpc.GetHeartBeatReply{})
	mockWorkerServer.EnqueueGetHeartbeatReply(rpc.GetHeartBeatReply{})
	mockWorkerServer.StartServer()
	workerId1 := WorkerId(0)
	workerId2 := WorkerId(1)
	masterState := MasterState{
		Workers: []Worker{
			{Id: workerId1, Hostname: mockWorkerServer.Hostname, Port: mockWorkerServer.Port, Status: Offline},
			{Id: workerId2, Hostname: mockWorkerServer.Hostname, Port: mockWorkerServer.Port, Status: Offline},
		},
	}

	UpdateWorkerTasksFromHeartbeats(&masterState, rpc.DefaultRpcGateway{})

	assert.Equal(t, Online, masterState.Workers[workerId1].Status)
	assert.Equal(t, Online, masterState.Workers[workerId2].Status)

	t.Cleanup(func() {
		mockWorkerServer.StopServer()
	})
}

func TestThat_ItShouldUpdateWorkerToOfflineState_WhenWorkerIsOfflineOnScanning(t *testing.T) {
	workerId1 := WorkerId(0)
	workerId2 := WorkerId(1)
	masterState := MasterState{
		Workers: []Worker{
			{Id: workerId1, Hostname: "localhost", Port: 6789, Status: Online},
			{Id: workerId2, Hostname: "localhost", Port: 6789, Status: Offline},
		},
	}

	UpdateWorkerTasksFromHeartbeats(&masterState, rpc.DefaultRpcGateway{})

	assert.Equal(t, Offline, masterState.Workers[workerId1].Status)
	assert.Equal(t, Offline, masterState.Workers[workerId2].Status)
}

func TestThat_ItShouldUpdateMapTaskStateAssignedToWorker_WhenOnlineWorkerHasUpdatedMapTask(t *testing.T) {
	mockWorkerServer := WorkerServer{}
	mockWorkerServer.StartServer()
	onlineWorkerId := WorkerId(0)
	offlineWorkerId := WorkerId(1)
	mapTaskId := uint16(0)
	masterState := MasterState{
		Workers: []Worker{
			{Id: onlineWorkerId, Hostname: mockWorkerServer.Hostname, Port: mockWorkerServer.Port, Status: Online},
			{Id: offlineWorkerId, Hostname: "localhost", Port: 1234, Status: Offline},
		},
		MapTasks: []MapTask{
			{
				Id:                     mapTaskId,
				Status:                 rpc.InProgress,
				InputFileSplitLocation: "//path/to/input-file-split",
				IntermediateFiles:      []rpc.IntermediateFile{},
				WorkerAssignedId:       &onlineWorkerId,
			},
		},
	}

	onlineWorkerHeartbeatReply := rpc.GetHeartBeatReply{
		MapTask: &rpc.MapTaskState{
			Id:     0,
			Status: rpc.Completed,
			IntermediateFiles: []rpc.IntermediateFile{
				{
					Location:    "//path/to/location",
					Partition:   1,
					SizeInBytes: 65536,
				},
			},
		},
	}
	mockWorkerServer.EnqueueGetHeartbeatReply(onlineWorkerHeartbeatReply)

	UpdateWorkerTasksFromHeartbeats(&masterState, rpc.DefaultRpcGateway{})

	assert.Equal(t,
		onlineWorkerHeartbeatReply.MapTask.IntermediateFiles,
		masterState.MapTasks[mapTaskId].IntermediateFiles,
	)
	assert.Equal(t, Online, masterState.Workers[onlineWorkerId].Status)
	assert.Equal(t, rpc.Completed, masterState.MapTasks[mapTaskId].Status)

	t.Cleanup(func() {
		mockWorkerServer.StopServer()
	})
}

func TestThat_ItShouldUpdateReduceTaskStateAssignedToWorker_WhenOnlineWorkerHasUpdatedReduceTask(t *testing.T) {
	mockWorkerServer := WorkerServer{}
	mockWorkerServer.StartServer()
	onlineWorkerId := WorkerId(0)
	offlineWorkerId := WorkerId(1)
	reduceTaskId := uint16(0)
	masterState := MasterState{
		Workers: []Worker{
			{Id: onlineWorkerId, Hostname: mockWorkerServer.Hostname, Port: mockWorkerServer.Port, Status: Online},
			{Id: offlineWorkerId, Hostname: "localhost", Port: 1234, Status: Offline},
		},
		ReduceTasks: []ReduceTask{
			{
				Id:                 reduceTaskId,
				Status:             rpc.InProgress,
				PartitionNumber:    0,
				OutputFileLocation: nil,
				WorkerAssignedId:   &onlineWorkerId,
			},
		},
	}

	onlineWorkerHeartbeatReply := rpc.GetHeartBeatReply{
		ReduceTask: &rpc.ReduceTaskState{
			Id:                 reduceTaskId,
			Status:             rpc.Completed,
			PartitionNumber:    3,
			OutputFileLocation: "//path/to/file",
		},
	}
	mockWorkerServer.EnqueueGetHeartbeatReply(onlineWorkerHeartbeatReply)

	UpdateWorkerTasksFromHeartbeats(&masterState, rpc.DefaultRpcGateway{})

	assert.Equal(t,
		onlineWorkerHeartbeatReply.ReduceTask.OutputFileLocation,
		*(masterState.ReduceTasks[reduceTaskId].OutputFileLocation),
	)
	assert.Equal(t, Online, masterState.Workers[onlineWorkerId].Status)
	assert.Equal(t, rpc.Completed, masterState.ReduceTasks[reduceTaskId].Status)
	assert.Equal(t, "//path/to/file", *masterState.ReduceTasks[reduceTaskId].OutputFileLocation)

	t.Cleanup(func() {
		mockWorkerServer.StopServer()
	})
}

func TestThat_ItShouldSendRequestToReduceInProgressWorkerToInterruptDuplicateTask_WhenAnotherWorkerIsAssignedToTask(
	t *testing.T,
) {
	mockWorkerServer := WorkerServer{}
	mockWorkerServer.StartServer()
	duplicateAssignedWorkerId := WorkerId(0)
	assignedWorkerId := WorkerId(1)
	reduceTaskId := uint16(0)
	masterState := MasterState{
		Workers: []Worker{
			{Id: duplicateAssignedWorkerId, Hostname: mockWorkerServer.Hostname, Port: mockWorkerServer.Port, Status: Offline},
			{Id: assignedWorkerId, Hostname: "localhost", Port: 1234, Status: Online},
		},
		ReduceTasks: []ReduceTask{
			{
				Id:                 reduceTaskId,
				Status:             rpc.InProgress,
				PartitionNumber:    0,
				OutputFileLocation: nil,
				WorkerAssignedId:   &assignedWorkerId,
			},
		},
	}

	heartbeatWithInProgressReduceTask := rpc.GetHeartBeatReply{
		ReduceTask: &rpc.ReduceTaskState{
			Id:              reduceTaskId,
			Status:          rpc.InProgress,
			PartitionNumber: 3,
		},
	}
	mockWorkerServer.EnqueueGetHeartbeatReply(heartbeatWithInProgressReduceTask)

	UpdateWorkerTasksFromHeartbeats(&masterState, rpc.DefaultRpcGateway{})

	assert.Nil(t, masterState.ReduceTasks[reduceTaskId].OutputFileLocation)
	assert.Equal(t, Online, masterState.Workers[duplicateAssignedWorkerId].Status)
	assert.Equal(t, Offline, masterState.Workers[assignedWorkerId].Status)
	assert.Equal(t, rpc.InProgress, masterState.ReduceTasks[reduceTaskId].Status)
	assert.Equal(t, assignedWorkerId, *masterState.ReduceTasks[reduceTaskId].WorkerAssignedId)

	assert.Equal(t, 2, mockWorkerServer.RequestCount)
	mockWorkerServer.TakeRequest()
	reduceTaskAbortionRequest := mockWorkerServer.TakeRequest()

	_, isAbortReduceTaskArgs := reduceTaskAbortionRequest.Args.(rpc.AbortReduceTaskArgs)
	assert.True(t, isAbortReduceTaskArgs)
	assert.Equal(t,
		fmt.Sprintf("%v:%v", mockWorkerServer.Hostname, mockWorkerServer.Port),
		reduceTaskAbortionRequest.Address,
	)

	t.Cleanup(func() {
		mockWorkerServer.StopServer()
	})
}

func TestThat_ItShouldNotSendRequestToReduceCompletedWorkerToInterruptDuplicateTask_WhenAnotherWorkerIsAssignedToTask(
	t *testing.T,
) {
	mockWorkerServer := WorkerServer{}
	mockWorkerServer.StartServer()
	duplicateAssignedWorkerId := WorkerId(0)
	assignedWorkerId := WorkerId(1)
	reduceTaskId := uint16(0)
	masterState := MasterState{
		Workers: []Worker{
			{Id: duplicateAssignedWorkerId, Hostname: mockWorkerServer.Hostname, Port: mockWorkerServer.Port, Status: Offline},
			{Id: assignedWorkerId, Hostname: "localhost", Port: 1234, Status: Online},
		},
		ReduceTasks: []ReduceTask{
			{
				Id:                 reduceTaskId,
				Status:             rpc.InProgress,
				PartitionNumber:    0,
				OutputFileLocation: nil,
				WorkerAssignedId:   &assignedWorkerId,
			},
		},
	}

	heartBeatWithCompletedReduceTask := rpc.GetHeartBeatReply{
		ReduceTask: &rpc.ReduceTaskState{
			Id:                 reduceTaskId,
			Status:             rpc.Completed,
			PartitionNumber:    3,
			OutputFileLocation: "//path/to/file",
		},
	}
	mockWorkerServer.EnqueueGetHeartbeatReply(heartBeatWithCompletedReduceTask)

	UpdateWorkerTasksFromHeartbeats(&masterState, rpc.DefaultRpcGateway{})

	assert.Nil(t, masterState.ReduceTasks[reduceTaskId].OutputFileLocation)
	assert.Equal(t, Online, masterState.Workers[duplicateAssignedWorkerId].Status)
	assert.Equal(t, Offline, masterState.Workers[assignedWorkerId].Status)
	assert.Equal(t, rpc.InProgress, masterState.ReduceTasks[reduceTaskId].Status)
	assert.Equal(t, assignedWorkerId, *masterState.ReduceTasks[reduceTaskId].WorkerAssignedId)

	assert.Equal(t, 1, mockWorkerServer.RequestCount)
	heartBeatRequest := mockWorkerServer.TakeRequest()

	_, isGetHeartbeatArgs := heartBeatRequest.Args.(rpc.GetHeartBeatArgs)
	assert.True(t, isGetHeartbeatArgs)

	t.Cleanup(func() {
		mockWorkerServer.StopServer()
	})
}
