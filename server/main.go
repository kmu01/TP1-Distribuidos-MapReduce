package main

import (
	"context"
	"log"
	"mapreduce-tp/mapreduce/protos"
	"net"

	"google.golang.org/grpc"
)

type myCoordinatorServer struct{
	protos.UnimplementedCoordinatorServer
}

// respondemos al RequestTask del worker
func (s *myCoordinatorServer) AssignTask(_ context.Context, in *protos.RequestTask) (*protos.GiveTask, error) {
	log.Print("[CLIENT] Assing Task");
	return &protos.GiveTask{TypeTask: "map"}, nil
}

func (s *myCoordinatorServer) FinishedTask(_ context.Context, in *protos.TaskResult) (*protos.Ack, error) {
	log.Print("[CLIENT] Finished Task");
	return &protos.Ack{Ack: "ack"}, nil
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