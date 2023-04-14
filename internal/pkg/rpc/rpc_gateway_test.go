package rpc // import "github.com/hakakutalua/mapreduce/internal/pkg/rpc"

import (
	"log"
	"net"
	"net/rpc"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestThat_CallInvokesRemoteFunction(t *testing.T) {
	go startServer("localhost:6789")
	defaultRpcGateway := DefaultRpcGateway{}
	var reply int

	err := defaultRpcGateway.Call("localhost:6789", "MethodTypeExample.RemoteFunctionDouble", 16, &reply)

	assert.Equal(t, nil, err)
	assert.Equal(t, 32, reply)
}

func startServer(address string) {
	l, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	methodTypeExample := new(MethodTypeExample)
	err = rpc.Register(methodTypeExample)
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}

		go func(conn net.Conn) {
			rpc.ServeConn(conn)
		}(conn)
	}
}

type MethodTypeExample struct{}

func (t *MethodTypeExample) RemoteFunctionDouble(args int, reply *int) error {
	*reply = args * 2
	return nil
}
