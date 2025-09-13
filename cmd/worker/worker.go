package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	config "mapreduce-tp/common"
	"mapreduce-tp/common/protos"
	mapreduceseq "mapreduce-tp/seq"
	"math/rand"
	"os"
	"plugin"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Task int32

type KeyValue struct {
	Key   string
	Value string
}

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

func write(path string, file_name string, contents string) {
	path_file := path + file_name
	f, err := os.OpenFile(path_file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("Error while opening file for writing: %s, err: %v", path_file, err)
		panic(err)
	}
	n, err := f.WriteString(contents)
	if err != nil {
		panic(err)
	}
	log.Print("Bytes written: ", n)
	f.Sync()
	defer f.Close()
}

func create_file(file_name string, path string) {
	path_file := path + file_name
	f, err := os.Create(path_file)
	if err != nil {
		panic(err)
	}
	defer f.Close()
}

func hashKey(key string, R int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % R
}

func get_parcial_file_name(task_id int32, number_file int) string {
	int_task_id := int(task_id)
	number_file_string := strconv.Itoa(number_file)
	strig_task_id := strconv.Itoa(int_task_id)
	file_name := "mr-" + strig_task_id + "-" + number_file_string + ".txt"
	return file_name
}

func create_parcial_files(reducers int32, task_id int32, path string) []string {
	var filenames []string
	index := 0
	for index < int(reducers) {
		file_name := get_parcial_file_name(task_id, index)
		create_file(file_name, path)
		filenames = append(filenames, file_name)
		index++
	}
	return filenames
}

func write_parcial_files(hash_lines map[int]string, reducers int32, task_id int32, path string) {
	index := 0
	for index < int(reducers) {
		contents := hash_lines[index]
		file_name := get_parcial_file_name(task_id, index)
		write(path, file_name, contents)
		index++
	}
}

func get_hash_lines(map_result []mapreduceseq.KeyValue, reducers int32) map[int]string {
	hash_lines := make(map[int]string)
	for _, v := range map_result {
		line_string := v.Key + " " + v.Value + "\n"
		num_hash := hashKey(v.Key, int(reducers))
		hash_lines[num_hash] += line_string
	}
	return hash_lines
}

func get_reduce_contents(contents []KeyValue) string {
	// Ordenar resultados alfabéticamente por clave (Para comparar con la version secuencial)
	sort.Slice(contents, func(i, j int) bool {
		return contents[i].Key < contents[j].Key
	})
	var lines []string
	for _, v := range contents {
		line_string := v.Key + " " + v.Value
		lines = append(lines, line_string)
	}
	result := strings.Join(lines, "\n")
	return result
}

func commit_map(map_result []mapreduceseq.KeyValue, path string, task_id int32, reducers int32) []string {
	filenames := create_parcial_files(reducers, task_id, path)
	hash_lines := get_hash_lines(map_result, reducers)
	write_parcial_files(hash_lines, reducers, task_id, path)
	log.Printf("Map commited")
	return filenames
}

func run_reduce(file string) []KeyValue {
	file_path := parcial_path + file
	content, err := os.ReadFile(file_path)
	if err != nil {
		log.Fatalf("cannot read %v: %v", file, err)
	}
	lines := strings.Split(string(content), "\n")
	var kvs []KeyValue
	for index := range lines {
		if lines[index] == "" {
			continue
		}
		splits := strings.Split(lines[index], " ")
		kv := KeyValue{Key: splits[0], Value: splits[1]}
		kvs = append(kvs, kv)
	}
	return kvs
}

func exec_map(ctx context.Context, connection protos.CoordinatorClient, map_function func(string, string) []mapreduceseq.KeyValue, result *protos.GiveTask) {
	log.Print("MAP task assigned")
	map_result := run_map(result.Files[0], map_function)
	file_names := commit_map(map_result, parcial_path, result.TaskId, result.Reducers)
	_, err := connection.FinishedTask(ctx, &protos.TaskResult{WorkerId: result.WorkerId, FileNames: file_names})
	if err != nil {
		log.Fatalf("could not map, reduce_map might been finished:: %v", err)
	}
}

func get_final_file_name(task_id int32) string {
	strig_task_id := strconv.Itoa(int(task_id))
	file_name := "mr-out-" + strig_task_id + ".txt"
	return file_name
}

func commit_reduce(contents []KeyValue, path string, task_id int32) {
	file_name := get_final_file_name(task_id)
	result := get_reduce_contents(contents)
	write(path, file_name, result)
	log.Printf("Reduce commited: %s", file_name)
}

func exec_reduce(ctx context.Context, connection protos.CoordinatorClient, reduce_function func(string, []string) string, result *protos.GiveTask) {
	log.Print("REDUCE task assigned")
	/*
				Archivos parciales
				mr-0-0.txt: hola 1, mundo 1, hola 1
				mr-0-1.txt: mundo 1, hola 1

				mr-0-0.txt: hola archivo1.txt, mundo archivo2.txt, hola archivo2.txt
		        mr-0-1.txt: mundo archivo1.txt, hola archivo3.txt
	*/

	// Lee estos archivos parciales y junta los pares KeyValue
	var allKVs []KeyValue
	for _, file := range result.Files {
		kvs := run_reduce(file)
		allKVs = append(allKVs, kvs...)
	}

	/*
				allKVs = [("hola", "1"),
					("mundo", "1"),
					("hola", "1"),
					("mundo", "1"),
					("hola", "1")]

				allKVs = [("hola", "archivo1.txt"),
		                  ("mundo", "archivo2.txt"),
		                  ("hola", "archivo2.txt"),
		                  ("mundo", "archivo1.txt"),
		                  ("hola", "archivo3.txt")]
	*/

	// Se agrupa por palabra
	groups := make(map[string][]string)
	for _, kv := range allKVs {
		groups[kv.Key] = append(groups[kv.Key], kv.Value)
	}
	/*
				groups["hola"] = ["1", "1", "1"]
				groups["mundo"] = ["1", "1"]

				groups["hola"] = ["archivo1.txt", "archivo2.txt", "archivo3.txt"]
		        groups["mundo"] = ["archivo2.txt", "archivo1.txt"]
	*/

	// Invocar el reduce del plugin
	var finalResults []KeyValue
	for k, v := range groups {
		output := reduce_function(k, v)
		finalResults = append(finalResults, KeyValue{Key: k, Value: output})
	}
	/*
				hola 3
				mundo 2

				hola archivo1.txt,archivo2.txt,archivo3.txt
		        mundo archivo1.txt,archivo2.txt
	*/

	// Orden alfabetico
	sort.Slice(finalResults, func(i, j int) bool {
		return finalResults[i].Key < finalResults[j].Key
	})

	// Commiteo
	commit_reduce(finalResults, result_path, result.TaskId)
	_, err := connection.FinishedTask(ctx, &protos.TaskResult{WorkerId: result.WorkerId})
	if err != nil {
		log.Fatal("Connection with coordinator Failed: Tasks might be done.")
	}
}

func worker_failed(failure_prob int32) bool {
	v := int32(rand.Intn(101))
	return v < failure_prob
}

func run_worker(ctx context.Context, connection protos.CoordinatorClient, map_function func(string, string) []mapreduceseq.KeyValue, reduce_function func(string, []string) string, failure_prob int32) {
	var still_working bool = true
	for still_working {
		log.Print("Asking for new task")
		result, err := connection.AssignTask(ctx, &protos.RequestTask{})
		if err != nil {
			log.Fatal("Connection with coordinator Failed: Tasks might be done.")
		}

		if worker_failed(failure_prob) {
			log.Print("Worker Failed. EXIT")
			break
		}

		switch result.TypeTask {
		case int32(config.Map):
			exec_map(ctx, connection, map_function, result)
		case int32(config.Reduce):
			exec_reduce(ctx, connection, reduce_function, result)
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

	// busca funcion map
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v: %v", pluginFile, err)
	}
	mapf, ok := xmapf.(func(string, string) []mapreduceseq.KeyValue)
	if !ok {
		log.Fatalf("Map has wrong signature in %v", pluginFile)
	}

	// busca funcion reduce
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

	failure_prob := int32(0)
	if len(os.Args) == 3 {
		i, err := strconv.Atoi(os.Args[2])
		if err != nil || i > 100 || i < 0 {
			failure_prob = 0
		}
		failure_prob = int32(i)
	}

	pluginFile := os.Args[1]

	map_function, reduce_function := get_map_reduce_functions(pluginFile)
	ctx, connection := connect()
	run_worker(ctx, connection, map_function, reduce_function, failure_prob)
}
	