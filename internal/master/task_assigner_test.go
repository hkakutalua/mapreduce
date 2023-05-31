package master

import (
	"testing"

	"github.com/hkakutalua/mapreduce/internal/pkg/rpc"
	"github.com/stretchr/testify/assert"
)

func TestThat_ItShouldAssignOneMapTaskForEachWorker_WhenWorkersAreOnlineAndTasksAreIdle(t *testing.T) {
	mockWorkerServer := WorkerServer{}
	mockWorkerServer.StartServer()
	availableWorker1 := Worker{
		Id:       WorkerId(0),
		Hostname: mockWorkerServer.Hostname,
		Port:     mockWorkerServer.Port,
		Status:   Online,
	}
	availableWorker2 := Worker{
		Id:       WorkerId(1),
		Hostname: mockWorkerServer.Hostname,
		Port:     mockWorkerServer.Port,
		Status:   Online,
	}
	mapTask1 := MapTask{
		Id:                     0,
		Status:                 rpc.Idle,
		InputFileSplitLocation: "//path/to/split_1",
		WorkerAssignedId:       nil,
	}
	mapTask2 := MapTask{
		Id:                     1,
		Status:                 rpc.Idle,
		InputFileSplitLocation: "//path/to/split_2",
		WorkerAssignedId:       nil,
	}
	mapTask3 := MapTask{
		Id:                     2,
		Status:                 rpc.Idle,
		InputFileSplitLocation: "//path/to/split_3",
		WorkerAssignedId:       nil,
	}
	masterState := MasterState{
		Workers:  []Worker{availableWorker1, availableWorker2},
		MapTasks: []MapTask{mapTask1, mapTask2, mapTask3},
	}
	mockWorkerServer.EnqueueStartMapTask(rpc.StartMapTaskReply{})
	mockWorkerServer.EnqueueStartMapTask(rpc.StartMapTaskReply{})

	AssignIdleMapTasksToAvailableWorkers(&masterState, rpc.DefaultRpcGateway{})

	assert.Equal(t, rpc.InProgress, masterState.MapTasks[mapTask1.Id].Status)
	assert.Equal(t, availableWorker1.Id, *masterState.MapTasks[mapTask1.Id].WorkerAssignedId)
	assert.Equal(t, rpc.InProgress, masterState.MapTasks[mapTask2.Id].Status)
	assert.Equal(t, availableWorker2.Id, *masterState.MapTasks[mapTask2.Id].WorkerAssignedId)
	assert.Equal(t, rpc.Idle, masterState.MapTasks[mapTask3.Id].Status)
	assert.Nil(t, masterState.MapTasks[mapTask3.Id].WorkerAssignedId)

	assert.Equal(t, 2, mockWorkerServer.RequestCount)
	startMapTaskArgs1 := mockWorkerServer.TakeRequest().Args
	startMapTaskArgs2 := mockWorkerServer.TakeRequest().Args
	assert.IsType(t, rpc.StartMapTaskArgs{}, startMapTaskArgs1)
	assert.IsType(t, rpc.StartMapTaskArgs{}, startMapTaskArgs2)
	assert.Equal(t, uint16(0), startMapTaskArgs1.(rpc.StartMapTaskArgs).Id)
	assert.Equal(t, "//path/to/split_1", startMapTaskArgs1.(rpc.StartMapTaskArgs).InputFileSplitLocation)
	assert.Equal(t, uint16(1), startMapTaskArgs2.(rpc.StartMapTaskArgs).Id)
	assert.Equal(t, "//path/to/split_2", startMapTaskArgs2.(rpc.StartMapTaskArgs).InputFileSplitLocation)

	t.Cleanup(func() {
		mockWorkerServer.StopServer()
	})
}

func TestThat_ItShouldNotAssignMapTasks_WhenWorkersAreOffline(t *testing.T) {
	mockWorkerServer := WorkerServer{}
	mockWorkerServer.StartServer()
	offlineWorker1 := Worker{
		Id:       WorkerId(0),
		Hostname: mockWorkerServer.Hostname,
		Port:     mockWorkerServer.Port,
		Status:   Offline,
	}
	offlineWorker2 := Worker{
		Id:       WorkerId(1),
		Hostname: mockWorkerServer.Hostname,
		Port:     mockWorkerServer.Port,
		Status:   Offline,
	}
	mapTask1 := MapTask{
		Id:                     0,
		Status:                 rpc.Idle,
		InputFileSplitLocation: "//path/to/split_1",
		WorkerAssignedId:       nil,
	}
	mapTask2 := MapTask{
		Id:                     1,
		Status:                 rpc.Idle,
		InputFileSplitLocation: "//path/to/split_2",
		WorkerAssignedId:       nil,
	}
	mapTask3 := MapTask{
		Id:                     2,
		Status:                 rpc.Idle,
		InputFileSplitLocation: "//path/to/split_3",
		WorkerAssignedId:       nil,
	}
	masterState := MasterState{
		Workers:  []Worker{offlineWorker1, offlineWorker2},
		MapTasks: []MapTask{mapTask1, mapTask2, mapTask3},
	}

	AssignIdleMapTasksToAvailableWorkers(&masterState, rpc.DefaultRpcGateway{})

	assert.Equal(t, rpc.Idle, masterState.MapTasks[mapTask1.Id].Status)
	assert.Nil(t, masterState.MapTasks[mapTask1.Id].WorkerAssignedId)
	assert.Equal(t, rpc.Idle, masterState.MapTasks[mapTask2.Id].Status)
	assert.Nil(t, masterState.MapTasks[mapTask2.Id].WorkerAssignedId)
	assert.Equal(t, rpc.Idle, masterState.MapTasks[mapTask3.Id].Status)
	assert.Nil(t, masterState.MapTasks[mapTask3.Id].WorkerAssignedId)

	assert.Equal(t, 0, mockWorkerServer.RequestCount)

	t.Cleanup(func() {
		mockWorkerServer.StopServer()
	})
}

func TestThat_ItShouldNotAssignMapTask_WhenWorkersHaveInProgressMapTask(t *testing.T) {
	mockWorkerServer := WorkerServer{}
	mockWorkerServer.StartServer()
	workerWithAssignedTask1 := Worker{
		Id:       WorkerId(0),
		Hostname: mockWorkerServer.Hostname,
		Port:     mockWorkerServer.Port,
		Status:   Online,
	}
	workerWithAssignedTask2 := Worker{
		Id:       WorkerId(1),
		Hostname: mockWorkerServer.Hostname,
		Port:     mockWorkerServer.Port,
		Status:   Online,
	}
	mapTask1 := MapTask{
		Id:                     0,
		Status:                 rpc.InProgress,
		InputFileSplitLocation: "//path/to/split_1",
		WorkerAssignedId:       &workerWithAssignedTask1.Id,
	}
	mapTask2 := MapTask{
		Id:                     1,
		Status:                 rpc.InProgress,
		InputFileSplitLocation: "//path/to/split_2",
		WorkerAssignedId:       &workerWithAssignedTask2.Id,
	}
	mapTask3 := MapTask{
		Id:                     2,
		Status:                 rpc.Idle,
		InputFileSplitLocation: "//path/to/split_3",
		WorkerAssignedId:       nil,
	}
	masterState := MasterState{
		Workers:  []Worker{workerWithAssignedTask1, workerWithAssignedTask2},
		MapTasks: []MapTask{mapTask1, mapTask2, mapTask3},
	}

	AssignIdleMapTasksToAvailableWorkers(&masterState, rpc.DefaultRpcGateway{})

	assert.Equal(t, rpc.InProgress, masterState.MapTasks[mapTask1.Id].Status)
	assert.Equal(t, workerWithAssignedTask1.Id, *masterState.MapTasks[mapTask1.Id].WorkerAssignedId)
	assert.Equal(t, rpc.InProgress, masterState.MapTasks[mapTask2.Id].Status)
	assert.Equal(t, workerWithAssignedTask2.Id, *masterState.MapTasks[mapTask2.Id].WorkerAssignedId)
	assert.Equal(t, rpc.Idle, masterState.MapTasks[mapTask3.Id].Status)
	assert.Nil(t, masterState.MapTasks[mapTask3.Id].WorkerAssignedId)

	assert.Equal(t, 0, mockWorkerServer.RequestCount)

	t.Cleanup(func() {
		mockWorkerServer.StopServer()
	})
}

func TestThat_ItShouldAssignOneMapTaskForEachWorker_WhenWorkersAreOnlineAndHaveAllTheirAssignedTasksAsCompleted(t *testing.T) {
	mockWorkerServer := WorkerServer{}
	mockWorkerServer.StartServer()
	mockWorkerServer.EnqueueStartMapTask(rpc.StartMapTaskReply{})
	workerWithAssignedTask1 := Worker{
		Id:       WorkerId(0),
		Hostname: mockWorkerServer.Hostname,
		Port:     mockWorkerServer.Port,
		Status:   Online,
	}
	workerWithAssignedTask2 := Worker{
		Id:       WorkerId(1),
		Hostname: mockWorkerServer.Hostname,
		Port:     mockWorkerServer.Port,
		Status:   Online,
	}
	mapTask1 := MapTask{
		Id:                     0,
		Status:                 rpc.Completed,
		InputFileSplitLocation: "//path/to/split_1",
		WorkerAssignedId:       &workerWithAssignedTask1.Id,
	}
	mapTask2 := MapTask{
		Id:                     1,
		Status:                 rpc.Completed,
		InputFileSplitLocation: "//path/to/split_2",
		WorkerAssignedId:       &workerWithAssignedTask2.Id,
	}
	mapTask3 := MapTask{
		Id:                     2,
		Status:                 rpc.Completed,
		InputFileSplitLocation: "//path/to/split_2",
		WorkerAssignedId:       &workerWithAssignedTask2.Id,
	}
	mapTask4 := MapTask{
		Id:                     3,
		Status:                 rpc.Idle,
		InputFileSplitLocation: "//path/to/split_4",
		WorkerAssignedId:       nil,
	}
	masterState := MasterState{
		Workers:  []Worker{workerWithAssignedTask1, workerWithAssignedTask2},
		MapTasks: []MapTask{mapTask1, mapTask2, mapTask3, mapTask4},
	}

	AssignIdleMapTasksToAvailableWorkers(&masterState, rpc.DefaultRpcGateway{})

	assert.Equal(t, rpc.Completed, masterState.MapTasks[mapTask1.Id].Status)
	assert.Equal(t, workerWithAssignedTask1.Id, *masterState.MapTasks[mapTask1.Id].WorkerAssignedId)
	assert.Equal(t, rpc.Completed, masterState.MapTasks[mapTask2.Id].Status)
	assert.Equal(t, workerWithAssignedTask2.Id, *masterState.MapTasks[mapTask2.Id].WorkerAssignedId)
	assert.Equal(t, rpc.Completed, masterState.MapTasks[mapTask3.Id].Status)
	assert.Equal(t, workerWithAssignedTask2.Id, *masterState.MapTasks[mapTask3.Id].WorkerAssignedId)
	assert.Equal(t, rpc.InProgress, masterState.MapTasks[mapTask4.Id].Status)
	assert.Equal(t, workerWithAssignedTask1.Id, *masterState.MapTasks[mapTask4.Id].WorkerAssignedId)

	assert.Equal(t, 1, mockWorkerServer.RequestCount)
	startMapTaskArgs1 := mockWorkerServer.TakeRequest().Args
	assert.IsType(t, rpc.StartMapTaskArgs{}, startMapTaskArgs1)
	assert.Equal(t, uint16(3), startMapTaskArgs1.(rpc.StartMapTaskArgs).Id)
	assert.Equal(t, "//path/to/split_4", startMapTaskArgs1.(rpc.StartMapTaskArgs).InputFileSplitLocation)

	t.Cleanup(func() {
		mockWorkerServer.StopServer()
	})
}

func TestThat_ItShouldAssignReduceTaskForAvailableWorker_WhenAllMapTasksAreCompleted(t *testing.T) {
	mockWorkerServer := WorkerServer{}
	mockWorkerServer.StartServer()
	availableWorker1 := Worker{
		Id:       WorkerId(0),
		Hostname: mockWorkerServer.Hostname,
		Port:     mockWorkerServer.Port,
		Status:   Online,
	}
	availableWorker2 := Worker{
		Id:       WorkerId(1),
		Hostname: mockWorkerServer.Hostname,
		Port:     mockWorkerServer.Port,
		Status:   Online,
	}
	completedMapTask1 := MapTask{
		Id:                     0,
		Status:                 rpc.Completed,
		InputFileSplitLocation: "//path/to/split_1",
		WorkerAssignedId:       nil,
		IntermediateFiles: []rpc.IntermediateFile{
			{Location: "//path/to/0/file_1", Partition: 0, SizeInBytes: 1_000},
			{Location: "//path/to/0/file_2", Partition: 1, SizeInBytes: 1_000},
		},
	}
	completedMapTask2 := MapTask{
		Id:                     1,
		Status:                 rpc.Completed,
		InputFileSplitLocation: "//path/to/split_2",
		WorkerAssignedId:       nil,
		IntermediateFiles: []rpc.IntermediateFile{
			{Location: "//path/to/1/file_1", Partition: 0, SizeInBytes: 1_000},
			{Location: "//path/to/1/file_2", Partition: 1, SizeInBytes: 1_000},
		},
	}
	idleReduceTask1 := ReduceTask{Id: 0, Status: rpc.Idle, PartitionNumber: 0}
	idleReduceTask2 := ReduceTask{Id: 1, Status: rpc.Idle, PartitionNumber: 1}

	masterState := MasterState{
		Workers:     []Worker{availableWorker1, availableWorker2},
		MapTasks:    []MapTask{completedMapTask1, completedMapTask2},
		ReduceTasks: []ReduceTask{idleReduceTask1, idleReduceTask2},
	}

	mockWorkerServer.EnqueueStartReduceTask(rpc.StartReduceTaskReply{})
	mockWorkerServer.EnqueueStartReduceTask(rpc.StartReduceTaskReply{})

	AssignIdleReduceTasksToAvailableWorkers(&masterState, rpc.DefaultRpcGateway{})

	assert.Equal(t, 2, mockWorkerServer.RequestCount)

	assert.Equal(t, rpc.InProgress, masterState.ReduceTasks[idleReduceTask1.Id].Status)
	assert.Equal(t, availableWorker1.Id, *masterState.ReduceTasks[idleReduceTask1.Id].WorkerAssignedId)
	assert.Equal(t, rpc.InProgress, masterState.ReduceTasks[idleReduceTask2.Id].Status)
	assert.Equal(t, availableWorker2.Id, *masterState.ReduceTasks[idleReduceTask2.Id].WorkerAssignedId)

	startReduceTaskArgs1 := mockWorkerServer.TakeRequest().Args
	assert.IsType(t, rpc.StartReduceTaskArgs{}, startReduceTaskArgs1)
	assert.Equal(t, uint16(0), startReduceTaskArgs1.(rpc.StartReduceTaskArgs).Id)
	assert.Equal(t,
		[]rpc.IntermediateFile{
			{Location: "//path/to/0/file_1", Partition: 0, SizeInBytes: 1_000},
			{Location: "//path/to/1/file_1", Partition: 0, SizeInBytes: 1_000},
		},
		startReduceTaskArgs1.(rpc.StartReduceTaskArgs).IntermediateFiles,
	)

	startReduceTaskArgs2 := mockWorkerServer.TakeRequest().Args
	assert.IsType(t, rpc.StartReduceTaskArgs{}, startReduceTaskArgs2)
	assert.Equal(t, uint16(1), startReduceTaskArgs2.(rpc.StartReduceTaskArgs).Id)
	assert.Equal(t,
		[]rpc.IntermediateFile{
			{Location: "//path/to/0/file_2", Partition: 1, SizeInBytes: 1_000},
			{Location: "//path/to/1/file_2", Partition: 1, SizeInBytes: 1_000},
		},
		startReduceTaskArgs2.(rpc.StartReduceTaskArgs).IntermediateFiles,
	)

	t.Cleanup(func() {
		mockWorkerServer.StopServer()
	})
}

func TestThat_ItShouldNotAssignReduceTaskForAvailableWorker_WhenNotAllMapTasksCompleted(t *testing.T) {
	mockWorkerServer := WorkerServer{}
	mockWorkerServer.StartServer()
	availableWorker1 := Worker{
		Id:       WorkerId(0),
		Hostname: mockWorkerServer.Hostname,
		Port:     mockWorkerServer.Port,
		Status:   Online,
	}
	availableWorker2 := Worker{
		Id:       WorkerId(1),
		Hostname: mockWorkerServer.Hostname,
		Port:     mockWorkerServer.Port,
		Status:   Online,
	}
	completedMapTask := MapTask{
		Id:                     0,
		Status:                 rpc.Completed,
		InputFileSplitLocation: "//path/to/split_1",
		WorkerAssignedId:       nil,
		IntermediateFiles: []rpc.IntermediateFile{
			{Location: "//path/to/0/file_1", Partition: 0, SizeInBytes: 1_000},
			{Location: "//path/to/0/file_2", Partition: 1, SizeInBytes: 1_000},
		},
	}
	inProgressMapTask := MapTask{
		Id:                     1,
		Status:                 rpc.InProgress,
		InputFileSplitLocation: "//path/to/split_2",
		WorkerAssignedId:       nil,
		IntermediateFiles: []rpc.IntermediateFile{
			{Location: "//path/to/1/file_1", Partition: 0, SizeInBytes: 1_000},
		},
	}
	reduceTask1 := ReduceTask{Id: 0, Status: rpc.Idle, PartitionNumber: 0}
	reduceTask2 := ReduceTask{Id: 1, Status: rpc.Idle, PartitionNumber: 1}

	masterState := MasterState{
		Workers:     []Worker{availableWorker1, availableWorker2},
		MapTasks:    []MapTask{completedMapTask, inProgressMapTask},
		ReduceTasks: []ReduceTask{reduceTask1, reduceTask2},
	}

	AssignIdleReduceTasksToAvailableWorkers(&masterState, rpc.DefaultRpcGateway{})

	assert.Equal(t, 0, mockWorkerServer.RequestCount)

	assert.Equal(t, rpc.Idle, masterState.ReduceTasks[reduceTask1.Id].Status)
	assert.Nil(t, masterState.ReduceTasks[reduceTask1.Id].WorkerAssignedId)
	assert.Equal(t, rpc.Idle, masterState.ReduceTasks[reduceTask2.Id].Status)
	assert.Nil(t, masterState.ReduceTasks[reduceTask2.Id].WorkerAssignedId)

	t.Cleanup(func() {
		mockWorkerServer.StopServer()
	})
}
