package main

import (
	"context"
	"flag"
	"log"
	"mapreduce-tp/mapreduce/protos"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	addr = flag.String("addr", "localhost:8090", "the address to connect to")
)

func main(){
	flag.Parse();
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()));
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	ctx := context.Background();
	connection := protos.NewCoordinatorClient(conn);
	result , err := connection.AssignTask(ctx, &protos.RequestTask{WorkerId:1 });
	if err != nil{
		log.Fatalf("could not connect: %v", err)
	}

	if result.TypeTask == "map" {
		//TODO: hacer todo el mapeo y quedarte con un dict

		r, err := connection.FinishedTask(ctx, &protos.TaskResult{Results:"done" });
		if err != nil{
		log.Fatalf("could not map: %v", err)
		}
		log.Print("resultado %s",r)
	} else if result.TypeTask == "reduce" {
		//TODO:
	} else {
		//TODO:
	}
}