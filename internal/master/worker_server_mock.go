package master

import (
	"fmt"
	"log"
	"net"
	golangrpc "net/rpc"

	"github.com/hkakutalua/mapreduce/internal/pkg/rpc"
)

type Request struct {
	Args    interface{}
	Address string
}

type WorkerServer struct {
	connections                    []net.Conn
	listener                       net.Listener
	requests                       []Request
	enqueuedGetHearbeatReplies     []rpc.GetHeartBeatReply
	enqueuedAbortReduceTaskReplies []rpc.AbortReduceTaskReply
	enqueuedStartMapTaskReplies    []rpc.StartMapTaskReply
	server                         golangrpc.Server
	Hostname                       string
	Port                           uint16
	RequestCount                   int
}

func (workerServer *WorkerServer) EnqueueGetHeartbeatReply(reply rpc.GetHeartBeatReply) {
	workerServer.enqueuedGetHearbeatReplies =
		append(workerServer.enqueuedGetHearbeatReplies, reply)
}

func (workerServer *WorkerServer) EnqueueStartMapTask(reply rpc.StartMapTaskReply) {
	workerServer.enqueuedStartMapTaskReplies =
		append(workerServer.enqueuedStartMapTaskReplies, reply)
}

func (workerServer *WorkerServer) EnqueueAbortReduceTaskReply(reply rpc.AbortReduceTaskReply) {
	workerServer.enqueuedAbortReduceTaskReplies =
		append(workerServer.enqueuedAbortReduceTaskReplies, reply)
}

func (workerServer *WorkerServer) TakeRequest() Request {
	if len(workerServer.requests) == 0 {
		log.Panicf("No requests were sent")
	}

	request := workerServer.requests[0]
	workerServer.requests = workerServer.requests[1:]
	workerServer.RequestCount--
	return request
}

func (workerServer *WorkerServer) GetHeartbeat(
	args rpc.GetHeartBeatArgs,
	reply *rpc.GetHeartBeatReply,
) error {
	workerServer.addRequest(args)

	if len(workerServer.enqueuedGetHearbeatReplies) == 0 {
		return fmt.Errorf("no more enqueued heartbeats")
	}

	*reply = workerServer.enqueuedGetHearbeatReplies[0]

	workerServer.enqueuedGetHearbeatReplies = workerServer.enqueuedGetHearbeatReplies[1:]
	return nil
}

func (workerServer *WorkerServer) StartMapTask(
	args rpc.StartMapTaskArgs,
	reply *rpc.StartMapTaskReply,
) error {
	workerServer.addRequest(args)

	if len(workerServer.enqueuedStartMapTaskReplies) == 0 {
		return fmt.Errorf("no more enqueued start map tasks")
	}

	*reply = workerServer.enqueuedStartMapTaskReplies[0]

	workerServer.enqueuedStartMapTaskReplies = workerServer.enqueuedStartMapTaskReplies[1:]
	return nil
}

func (workerServer *WorkerServer) AbortReduceTask(
	args rpc.AbortReduceTaskArgs,
	reply *rpc.AbortReduceTaskReply,
) error {
	workerServer.addRequest(args)

	if len(workerServer.enqueuedGetHearbeatReplies) == 0 {
		return fmt.Errorf("no more enqueued reduce task abortions")
	}

	*reply = workerServer.enqueuedAbortReduceTaskReplies[0]

	workerServer.enqueuedAbortReduceTaskReplies = workerServer.enqueuedAbortReduceTaskReplies[1:]
	return nil
}

func (mockWorkerServer *WorkerServer) StartServer() {
	mockWorkerServer.Hostname = "localhost"
	mockWorkerServer.Port = 1025
	mockWorkerServer.server = golangrpc.Server{}
	address := fmt.Sprintf("%v:%v", mockWorkerServer.Hostname, mockWorkerServer.Port)

	l, err := net.Listen("tcp", address)
	mockWorkerServer.listener = l
	if err != nil {
		log.Fatal(err)
	}

	mockWorkerServer.server.Register(mockWorkerServer)
	if err != nil {
		log.Fatal(err)
	}

	go listenToConnections(mockWorkerServer, l)
}

func listenToConnections(mockWorkerServer *WorkerServer, listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Print(err)
			break
		}

		mockWorkerServer.connections = append(mockWorkerServer.connections, conn)

		go func(conn net.Conn) {
			mockWorkerServer.server.ServeConn(conn)
		}(conn)
	}
}

func (mockWorkerServer *WorkerServer) addRequest(args interface{}) {
	mockWorkerServer.requests = append(
		mockWorkerServer.requests,
		Request{Args: args, Address: fmt.Sprintf("%v:%v", mockWorkerServer.Hostname, mockWorkerServer.Port)},
	)

	mockWorkerServer.RequestCount++
}

func (mockWorkerServer *WorkerServer) StopServer() {
	mockWorkerServer.enqueuedGetHearbeatReplies = []rpc.GetHeartBeatReply{}
	for _, conn := range mockWorkerServer.connections {
		conn.Close()
	}

	mockWorkerServer.listener.Close()
}
