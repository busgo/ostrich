package main

import (
	"context"
	"github.com/busgo/ostrich/pkg/discovery"
	"github.com/busgo/ostrich/pkg/etcd"
	"github.com/busgo/ostrich/proto/pb"
	"google.golang.org/grpc"
	"log"
	"time"
)

func main() {

	cli, err := etcd.NewEtcdClient(etcd.WithDailTimeout(time.Second*10), etcd.WithEndpoints([]string{"127.0.0.1:2379"}))
	if err != nil {
		panic(err)
	}
	etcdDiscovery := discovery.NewEtcdDiscovery(cli)
	if err != nil {
		panic(err)
	}

	target := "com.busgo.registry.proto.PingService"

	conn, err := grpc.Dial(target, grpc.WithInsecure(), grpc.WithBalancer(grpc.RoundRobin(etcdDiscovery)))

	if err != nil {
		panic(err)
	}
	client := pb.NewPingServiceClient(conn)

	for {

		response, err := client.Ping(context.Background(), &pb.PingRequest{
			Name: "jack",
		})

		log.Printf("response :%v,err:%v\n", response, err)
		time.Sleep(time.Second * 5)

	}
}
