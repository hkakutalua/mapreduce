package rpc // import "github.com/hakakutalua/mapreduce/internal/pkg/rpc"

import (
	"fmt"
	"net"
	golangrpc "net/rpc"
	"time"
)

type RpcGateway interface {
	Call(address string, methodName string, args any, reply any) error
}

type DefaultRpcGateway struct {
}

func (rpcGateway DefaultRpcGateway) Call(
	address string,
	methodName string,
	args any,
	reply any,
) error {
	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("host '%v' is unreachable: \"%v\"", address, err)
	}

	client := golangrpc.NewClient(conn)

	return client.Call(methodName, args, reply)
}
