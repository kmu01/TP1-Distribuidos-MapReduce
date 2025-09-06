package main

import (
	"log"
	"mapreduce-tp/mapreduce/protos"
	"net"
	"google.golang.org/grpc"
)

type myCoordinatorServer struct{
	protos.UnimplementedCoordinatorServer
}

func main(){
	lis, err := net.Listen("tcp", ":8090");
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	coordinator := grpc.NewServer();
	service := &myCoordinatorServer{}
	protos.RegisterCoordinatorServer(coordinator, service);
	err = coordinator.Serve(lis)
	if err!=nil{
		log.Fatalf("cant serve: %s", err)
	}


}