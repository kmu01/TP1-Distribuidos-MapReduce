package server

import (
	"flag"
	"log"
	"net"

	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 8089, "The server port")
)

func main(){
	lis, err := net.Listen("tcp", ":8089");
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	coordinator := grpc.NewServer();
		
}