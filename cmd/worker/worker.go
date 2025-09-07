package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"mapreduce-tp/common/protos"
	mapreduceseq "mapreduce-tp/seq"
	"os"
	"plugin"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Task int32

const (
	Map    Task = 0
	Reduce Task = 1
	Finish Task = 2
)

var (
	addr = flag.String("addr", "localhost:8091", "the address to connect to")
)

const parcial_path = "mapreduce-tp/filesystem/parcial_result/"
const result_path = "mapreduce-tp/filesystem/final_result/"

func connect() (context.Context, protos.CoordinatorClient) {
	flag.Parse()
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	ctx := context.Background()
	connection := protos.NewCoordinatorClient(conn)
	return ctx, connection
}

func run_map(file string, map_function func(string, string) []mapreduceseq.KeyValue) []mapreduceseq.KeyValue {
	content, err := os.ReadFile(file)
	if err != nil {
		log.Fatalf("cannot read %v: %v", file, err)
	}
	kva := map_function(file, string(content))
	return kva
}

func run_reduce(file string, reduce_function func(string, []string) string) string {
	return "reduce result"
}

func write(path string, contents string) {
	log.Print("writing... ")
	log.Print("contents: ", contents)
	log.Print("path: ", path)
}

func commit_map(map_result []mapreduceseq.KeyValue, path string, task_id int32) {
	//TODO: verificar el id de la task result, por ahora las id de las task son todas iguales

	var lines []string
	for _, v := range map_result {
		line_string := v.Key + "" + v.Value
		lines = append(lines, line_string)
	}
	contents := strings.Join(lines, "\n")
	strig_task_id := strconv.Itoa(int(task_id))
	file_name := "p-" + strig_task_id + ".txt"
	file_path := path + file_name
	write(file_path, contents)
	log.Print("Map commited")
}

func commit_reduce(contents string, path string, task_id int32) {
	strig_task_id := strconv.Itoa(int(task_id))
	file_name := "r-" + strig_task_id + ".txt"
	file_path := path + file_name
	write(file_path, contents)
	log.Print("Reduce commited")
}

func run_worker(ctx context.Context, connection protos.CoordinatorClient, map_function func(string, string) []mapreduceseq.KeyValue, reduce_function func(string, []string) string) {
	var still_working bool = true
	for still_working {
		result, err := connection.AssignTask(ctx, &protos.RequestTask{WorkerId: 1})
		if err != nil {
			log.Fatalf("could not connect: %v", err)
		}
		switch result.TypeTask {
		case 0:
			map_result := run_map(result.File, map_function)
			r, err := connection.FinishedTask(ctx, &protos.TaskResult{WorkerId: 1}) //lo dejamos el id en 1 pero despues le asignamos una id posta
			if err != nil {
				log.Fatalf("could not map: %v", err)
			}
			if r.CommitState == 1 {
				commit_map(map_result, parcial_path, result.TaskId)
			}
		case 1:
			reduce_result := run_reduce(result.File, reduce_function)

			r, err := connection.FinishedTask(ctx, &protos.TaskResult{WorkerId: 1}) //lo dejamos el id en 1 pero despues le asignamos una id posta
			if err != nil {
				log.Fatalf("could not map: %v", err)
			}
			if r.CommitState == 1 {
				commit_reduce(reduce_result, result_path, result.TaskId)
			}
		default:
			still_working = false
		}
	}
}

func get_map_reduce_functions(pluginFile string) (func(string, string) []mapreduceseq.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(pluginFile)
	if err != nil {
		log.Fatalf("cannot load plugin %v: %v", pluginFile, err)
	}

	// Buscar función Map
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v: %v", pluginFile, err)
	}
	mapf, ok := xmapf.(func(string, string) []mapreduceseq.KeyValue)
	if !ok {
		log.Fatalf("Map has wrong signature in %v", pluginFile)
	}

	// Buscar función Reduce
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v: %v", pluginFile, err)
	}
	reducef, ok := xreducef.(func(string, []string) string)
	if !ok {
		log.Fatalf("Reduce has wrong signature in %v", pluginFile)
	}
	return mapf, reducef
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: go run mainseq.go plugin.so")
		os.Exit(1)
	}

	pluginFile := os.Args[1]

	map_function, reduce_function := get_map_reduce_functions(pluginFile)
	ctx, connection := connect()
	run_worker(ctx, connection, map_function, reduce_function)

}
