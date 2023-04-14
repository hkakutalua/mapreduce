package rpc

type TaskStatus uint8

const (
	Idle       TaskStatus = 0
	InProgress TaskStatus = 1
	Completed  TaskStatus = 2
)

type IntermediateFile struct {
	Location    string
	Partition   uint16
	SizeInBytes uint64
}

type GetHeartBeatArgs struct{}
type GetHeartBeatReply struct {
	MapTask    *MapTaskState
	ReduceTask *ReduceTaskState
}

type AbortReduceTaskArgs struct{}
type AbortReduceTaskReply struct{}

type MapTaskState struct {
	Id                uint16
	Status            TaskStatus
	IntermediateFiles []IntermediateFile
}

type ReduceTaskState struct {
	Id                 uint16
	Status             TaskStatus
	PartitionNumber    uint16
	OutputFileLocation string
}
