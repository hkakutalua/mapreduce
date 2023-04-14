package master

import (
	"testing"

	"github.com/hkakutalua/mapreduce/internal/pkg/rpc"
	"github.com/stretchr/testify/assert"
)

func TestThat_ItShouldChangeWorkerOnlineStatus(t *testing.T) {
	workerId := WorkerId(1)
	masterState := MasterState{
		Workers: []Worker{
			{Id: WorkerId(0), Status: Offline},
			{Id: workerId, Status: Offline},
			{Id: WorkerId(2), Status: Offline},
		},
	}

	masterState.ChangeWorkerOnlineStatus(WorkerId(1), Online)

	assert.Equal(t, Online, masterState.Workers[workerId].Status)
	assert.Equal(t, Offline, masterState.Workers[WorkerId(0)].Status)
	assert.Equal(t, Offline, masterState.Workers[WorkerId(2)].Status)
}

func TestThat_ItShouldUpdateMapTaskAssignedToWorker_WhenOnlineWorkerHasMapTask(t *testing.T) {
	workerId := WorkerId(0)
	mapTaskId := uint16(0)
	masterState := MasterState{
		Workers: []Worker{
			{Id: workerId, Status: Offline},
			{Id: WorkerId(1), Status: Offline},
		},
		MapTasks: []MapTask{
			{
				Id:                     mapTaskId,
				Status:                 rpc.InProgress,
				InputFileSplitLocation: "//path/to/input-file-split",
				IntermediateFiles:      []rpc.IntermediateFile{},
				WorkerAssignedId:       &workerId,
			},
		},
	}
	intermediateFiles := []rpc.IntermediateFile{
		{
			Location:    "//path/to/location",
			Partition:   1,
			SizeInBytes: 65536,
		},
	}

	masterState.ChangeWorkerWithMapTaskToOnline(workerId, mapTaskId, intermediateFiles, rpc.Completed)

	assert.Equal(t,
		intermediateFiles,
		masterState.MapTasks[mapTaskId].IntermediateFiles,
	)
	assert.Equal(t, rpc.Completed, masterState.MapTasks[mapTaskId].Status)
	assert.Equal(t, Online, masterState.Workers[workerId].Status)
	assert.Equal(t, Offline, masterState.Workers[WorkerId(1)].Status)
}

func TestThat_ItShouldNotUpdateMapTask_WhenTheresNoWorkerAssignedToTask(t *testing.T) {
	workerId := WorkerId(0)
	mapTaskId := uint16(0)
	masterState := MasterState{
		Workers: []Worker{
			{Id: workerId, Status: Online},
			{Id: WorkerId(1), Status: Offline},
		},
		MapTasks: []MapTask{
			{
				Id:                     mapTaskId,
				Status:                 rpc.InProgress,
				InputFileSplitLocation: "//path/to/input-file-split",
				IntermediateFiles:      []rpc.IntermediateFile{},
				WorkerAssignedId:       nil,
			},
		},
	}
	intermediateFiles := []rpc.IntermediateFile{{Location: "//path/to/location"}}

	masterState.ChangeWorkerWithMapTaskToOnline(workerId, mapTaskId, intermediateFiles, rpc.Completed)

	assert.Equal(t, []rpc.IntermediateFile{}, masterState.MapTasks[mapTaskId].IntermediateFiles)
	assert.Equal(t, rpc.InProgress, masterState.MapTasks[mapTaskId].Status)
	assert.Nil(t, masterState.MapTasks[mapTaskId].WorkerAssignedId)
	assert.Equal(t, Online, masterState.Workers[workerId].Status)
	assert.Equal(t, Offline, masterState.Workers[WorkerId(1)].Status)
}

func TestThat_ItShouldNotUpdateMapTask_WhenAnotherWorkerHasBeenAssignedToTask(t *testing.T) {
	notAssignedWorker := WorkerId(0)
	assignedWorker := WorkerId(1)
	mapTaskId := uint16(0)
	masterState := MasterState{
		Workers: []Worker{
			{Id: notAssignedWorker, Status: Online},
			{Id: assignedWorker, Status: Offline},
		},
		MapTasks: []MapTask{
			{
				Id:                     mapTaskId,
				Status:                 rpc.InProgress,
				InputFileSplitLocation: "//path/to/input-file-split",
				IntermediateFiles:      []rpc.IntermediateFile{},
				WorkerAssignedId:       &assignedWorker,
			},
		},
	}
	intermediateFiles := []rpc.IntermediateFile{{Location: "//path/to/location"}}

	masterState.ChangeWorkerWithMapTaskToOnline(notAssignedWorker, mapTaskId, intermediateFiles, rpc.Completed)

	assert.Equal(t, []rpc.IntermediateFile{}, masterState.MapTasks[mapTaskId].IntermediateFiles)
	assert.Equal(t, rpc.InProgress, masterState.MapTasks[mapTaskId].Status)
	assert.Equal(t, assignedWorker, *masterState.MapTasks[mapTaskId].WorkerAssignedId)
	assert.Equal(t, Online, masterState.Workers[notAssignedWorker].Status)
	assert.Equal(t, Offline, masterState.Workers[assignedWorker].Status)
}

func TestThat_ItShouldNotUpdateMapTask_WhenTaskIsCompleted(t *testing.T) {
	workerId1 := WorkerId(0)
	workerId2 := WorkerId(1)
	mapTaskId := uint16(0)
	masterState := MasterState{
		Workers: []Worker{
			{Id: workerId1, Status: Online},
			{Id: workerId2, Status: Offline},
		},
		MapTasks: []MapTask{
			{
				Id:                     mapTaskId,
				Status:                 rpc.Completed,
				InputFileSplitLocation: "//path/to/input-file-split",
				IntermediateFiles:      []rpc.IntermediateFile{},
				WorkerAssignedId:       &workerId1,
			},
		},
	}
	intermediateFiles := []rpc.IntermediateFile{{Location: "//path/to/location"}}

	masterState.ChangeWorkerWithMapTaskToOnline(workerId1, mapTaskId, intermediateFiles, rpc.Completed)

	assert.Equal(t, []rpc.IntermediateFile{}, masterState.MapTasks[mapTaskId].IntermediateFiles)
	assert.Equal(t, rpc.Completed, masterState.MapTasks[mapTaskId].Status)
	assert.Equal(t, workerId1, *masterState.MapTasks[mapTaskId].WorkerAssignedId)
	assert.Equal(t, Online, masterState.Workers[workerId1].Status)
	assert.Equal(t, Offline, masterState.Workers[workerId2].Status)
}

func TestThat_ItShouldUpdateReduceTaskAssignedToWorker_WhenOnlineWorkerHasReduceTask(t *testing.T) {
	workerId := WorkerId(0)
	reduceTaskId := uint16(0)
	masterState := MasterState{
		Workers: []Worker{
			{Id: workerId, Status: Offline},
			{Id: WorkerId(1), Status: Offline},
		},
		ReduceTasks: []ReduceTask{
			{
				Id:                 reduceTaskId,
				Status:             rpc.InProgress,
				PartitionNumber:    3,
				OutputFileLocation: nil,
				AssignedWorkerId:   &workerId,
			},
		},
	}

	masterState.ChangeWorkerWithReduceTaskToOnline(
		workerId, reduceTaskId, "//path/to/file", rpc.Completed,
	)

	assert.Equal(t, Online, masterState.Workers[workerId].Status)
	assert.Equal(t, rpc.Completed, masterState.ReduceTasks[reduceTaskId].Status)
	assert.Equal(t, "//path/to/file", *masterState.ReduceTasks[reduceTaskId].OutputFileLocation)
}

func TestThat_ItShouldNotUpdateReduceTask_WhenTheresNoWorkerAssignedToTask(t *testing.T) {
	workerId := WorkerId(0)
	reduceTaskId := uint16(0)
	masterState := MasterState{
		Workers: []Worker{
			{Id: workerId, Status: Offline},
			{Id: WorkerId(1), Status: Offline},
		},
		ReduceTasks: []ReduceTask{
			{
				Id:                 reduceTaskId,
				Status:             rpc.InProgress,
				PartitionNumber:    3,
				OutputFileLocation: nil,
				AssignedWorkerId:   nil,
			},
		},
	}

	masterState.ChangeWorkerWithReduceTaskToOnline(
		workerId, reduceTaskId, "//path/to/file", rpc.Completed,
	)

	assert.Equal(t, Online, masterState.Workers[workerId].Status)
	assert.Equal(t, rpc.InProgress, masterState.ReduceTasks[reduceTaskId].Status)
	assert.Nil(t, masterState.ReduceTasks[reduceTaskId].OutputFileLocation)
	assert.Nil(t, masterState.ReduceTasks[reduceTaskId].AssignedWorkerId)
}

func TestThat_ItShouldNotUpdateReduceTask_WhenAnotherWorkerHasBeenAssignedToTask(t *testing.T) {
	nonAssignedWorkerId := WorkerId(0)
	assignedWorkerId := WorkerId(1)
	reduceTaskId := uint16(0)
	masterState := MasterState{
		Workers: []Worker{
			{Id: nonAssignedWorkerId, Status: Offline},
			{Id: assignedWorkerId, Status: Offline},
		},
		ReduceTasks: []ReduceTask{
			{
				Id:                 reduceTaskId,
				Status:             rpc.InProgress,
				PartitionNumber:    3,
				OutputFileLocation: nil,
				AssignedWorkerId:   &assignedWorkerId,
			},
		},
	}

	masterState.ChangeWorkerWithReduceTaskToOnline(
		nonAssignedWorkerId, reduceTaskId, "//path/to/file", rpc.Completed,
	)

	assert.Equal(t, Online, masterState.Workers[nonAssignedWorkerId].Status)
	assert.Equal(t, rpc.InProgress, masterState.ReduceTasks[reduceTaskId].Status)
	assert.Nil(t, masterState.ReduceTasks[reduceTaskId].OutputFileLocation)
	assert.Equal(t, assignedWorkerId, *masterState.ReduceTasks[reduceTaskId].AssignedWorkerId)
}

func TestThat_ItShouldNotUpdateReduceTask_WhenTaskIsCompleted(t *testing.T) {
	workerId := WorkerId(0)
	reduceTaskId := uint16(0)
	masterState := MasterState{
		Workers: []Worker{
			{Id: workerId, Status: Online},
			{Id: WorkerId(1), Status: Offline},
		},
		ReduceTasks: []ReduceTask{
			{
				Id:                 reduceTaskId,
				Status:             rpc.Completed,
				PartitionNumber:    3,
				OutputFileLocation: nil,
				AssignedWorkerId:   &workerId,
			},
		},
	}

	masterState.ChangeWorkerWithReduceTaskToOnline(
		workerId, reduceTaskId, "//path/to/file", rpc.InProgress,
	)

	assert.Equal(t, Online, masterState.Workers[workerId].Status)
	assert.Equal(t, rpc.Completed, masterState.ReduceTasks[reduceTaskId].Status)
	assert.Nil(t, masterState.ReduceTasks[reduceTaskId].OutputFileLocation)
	assert.Equal(t, workerId, *masterState.ReduceTasks[reduceTaskId].AssignedWorkerId)
}
