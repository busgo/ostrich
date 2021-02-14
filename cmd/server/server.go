package main

import (
	"context"
	"fmt"
	"github.com/busgo/ostrich/pkg/constant"
	"github.com/busgo/ostrich/pkg/etcd"
	"github.com/busgo/ostrich/pkg/registry"
	"github.com/busgo/ostrich/proto/pb"
	"google.golang.org/grpc"
	"net"
	"time"
)

type PingService struct {
}

func (s *PingService) Ping(context.Context, *pb.PingRequest) (*pb.PingResponse, error) {

	return &pb.PingResponse{Name: "java"}, nil
}

func (s *PingService) ServiceName() string {

	return "com.busgo.ostrich.proto.PingService"
}

func main() {

	initServer()
}

func init() {

}

func initServer() {

	e, err := etcd.NewEtcdClient(etcd.WithEndpoints([]string{"127.0.0.1:2379"}), etcd.WithDailTimeout(time.Second*10))

	if err != nil {
		panic(err)
	}

	reg := registry.NewEtcdRegistry(e)
	addr := fmt.Sprintf("127.0.0.1:8001")
	s := grpc.NewServer()
	pb.RegisterPingServiceServer(s, &PingService{})

	instance := &registry.ServiceInstance{
		Scheme:      constant.Scheme,
		ServiceName: "com.busgo.registry.proto.PingService",
		Ip:          "127.0.0.1",
		Port:        8001,
		Metadata:    "127.0.0.1:8001",
	}
	go reg.Register(instance, 10)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		reg.UnRegister(instance)
		panic(err)
	}

	err = s.Serve(l)
	if err != nil {
		reg.UnRegister(instance)
		panic(err)
	}
}
