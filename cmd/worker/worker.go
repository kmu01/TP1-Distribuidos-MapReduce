package main

import (
	"context"
	"fmt"
	"log"
	"mapreduce-tp/common/protos"
	mapreduceseq "mapreduce-tp/seq"
	"os"
	"plugin"
	"strconv"
	"strings"
	"sort"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Task int32

type KeyValue struct {
    Key   string
    Value string
}

const (
	Map    Task = 0
	Reduce Task = 1
	Finish Task = 2
)



const parcial_path = "filesystem/parcial_result/"
const result_path = "filesystem/final_result/"

func connect() (context.Context, protos.CoordinatorClient) {
	socketPath := "/tmp/mapreduce.sock"
	conn, err := grpc.Dial("unix://"+socketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

func run_reduce(file string, reduce_function func(string, []string) string) []KeyValue {
	log.Printf("file path in run_reduce %s jjjjjj", file)
	
	file_path := "filesystem/parcial_result/" + file

	content, err := os.ReadFile(file_path)

	log.Print("content in run_reduce: ",string(content))
	if err != nil {
		log.Fatalf("cannot read %v: %v", file, err)
	}

	lines := strings.Split(string(content), "\n")
	var kvs []KeyValue
	for index := range lines {
		splits := strings.Split(lines[index], ",")
		kv := KeyValue{Key:splits[0], Value:splits[1]}
		kvs = append(kvs, kv)
	}

	groups := make(map[string][]string)
	for _, kv := range kvs {
		groups[kv.Key] = append(groups[kv.Key], kv.Value)
	}

	var results []KeyValue
	for k, v := range groups {
		output := reduce_function(k, v)
		results = append(results, KeyValue{Key: k, Value: output})
	}

	// kva := reduce_function(file, string(groups))
	return results
	// return "resultados"
}

func write(path string, contents string) {
	log.Print("writing... ")
	f, err := os.Create(path)
	
	if err != nil {
        panic(err)
    }
	defer f.Close()

	n, err := f.WriteString(contents) 
	if err != nil {
        panic(err)
    }

	log.Print("contents in bytes: ", n)
	f.Sync()
}

func commit_map(map_result []mapreduceseq.KeyValue, path string, file_name string) {
	//TODO: verificar el id de la task result, por ahora las id de las task son todas iguales
	
	var lines []string
	for _, v := range map_result {
		line_string := v.Key + "," + v.Value
		lines = append(lines, line_string)
	}
	contents := strings.Join(lines, "\n")
	file_path := path + file_name
	write(file_path, contents)
	log.Printf("Map commited: %s", file_name)
}

func commit_reduce(contents []KeyValue, path string, file_name string) {
	// Ordenar resultados alfabéticamente por clave (Para comparar con la version secuencial)
	sort.Slice(contents, func(i, j int) bool {
		return contents[i].Key < contents[j].Key
	})
	var lines []string
	for _, v := range contents {
		line_string := v.Key + "," + v.Value
		lines = append(lines, line_string)
	}
	result := strings.Join(lines, "\n")
	file_path := path + file_name
	write(file_path, result)
	log.Print("Reduce commited")
}

func run_worker(ctx context.Context, connection protos.CoordinatorClient, map_function func(string, string) []mapreduceseq.KeyValue, reduce_function func(string, []string) string) {
	/*
		bug: realizar antes la escritura del archivo parcial y luego avisar al coordinador
        porque el coordinador piensa que la tarea se completo pero el worker no alcanzo a escribir el archivo
		y despues el reduce no encuentra el archivo parcial
	*/
	var still_working bool = true
       for still_working {
	       result, err := connection.AssignTask(ctx, &protos.RequestTask{})
	       if err != nil {
		       log.Fatalf("could not connect: %v", err)
	       }
	       switch result.TypeTask {
	       case 0:
		       map_result := run_map(result.File, map_function)
		       strig_task_id := strconv.Itoa(int(result.TaskId))
		       file_name := "p-" + strig_task_id + ".txt"
		       commit_map(map_result, parcial_path, file_name)
		       _, err := connection.FinishedTask(ctx, &protos.TaskResult{WorkerId: result.WorkerId, FileName: file_name})
		       if err != nil {
			       log.Fatalf("could not map: %v", err)
		       }
		case 1:
			reduce_result := run_reduce(result.File, reduce_function)

			strig_task_id := strconv.Itoa(int(result.TaskId))
			file_name := "r-" + strig_task_id + ".txt" // no lo va a usar el coordinador

			_, err := connection.FinishedTask(ctx, &protos.TaskResult{WorkerId: result.WorkerId, FileName: file_name}) //lo dejamos el id en 1 pero despues le asignamos una id posta
			if err != nil {
				log.Fatalf("could not map: %v", err)
			}
			commit_reduce(reduce_result, result_path, file_name)
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
