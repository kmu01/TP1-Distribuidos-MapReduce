package main

import (
	"context"
	"fmt"
	"log"
	config "mapreduce-tp/common"
	"mapreduce-tp/common/protos"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type Coordinator struct {
	workers           []Worker ///workers que estan activos
	map_tasks         []Task   ///completed and not completed
	reduce_tasks      map[int32]Task
	finish_map_reduce bool
	server            *grpc.Server
	next_worker_id    int32
	mu                sync.Mutex
	reducers          int32
}

type Task struct {
	task_id   int32
	task_type config.TaskType   // map/reduce/nothing
	status    config.TaskStatus // disponible/tomada/completada
	time      time.Time
	files     []string
}

type Worker struct {
	id   int32
	task *Task
}

var coordinator = Coordinator{}

type myCoordinatorServer struct {
	protos.UnimplementedCoordinatorServer
}

func init_coordinator(files []string) {
	coordinator.server = grpc.NewServer()
	coordinator.next_worker_id = 0
	coordinator.reducers = config.Reducers

	var id int32 = 0
	for _, filename := range files {
		log.Print("Added new file to coordinator: ", filename)
		files := []string{filename}
		new_task := Task{task_type: config.Map, status: config.Available, files: files, task_id: id}
		coordinator.map_tasks = append(coordinator.map_tasks, new_task)
		id += 1
	}
	coordinator.finish_map_reduce = false
	coordinator.reduce_tasks = make(map[int32]Task)

	for i := int32(0); i < coordinator.reducers; i++ {
		task := Task{
			task_id:   i,
			task_type: config.Reduce,
			status:    config.Available,
			files:     []string{},
		}
		coordinator.reduce_tasks[i] = task
	}
}

// respondemos al RequestTask del worker
func (s *myCoordinatorServer) AssignTask(ctx context.Context, in *protos.RequestTask) (*protos.GiveTask, error) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	coordinator.next_worker_id += 1

	var worker Worker
	var task = fetch_task()
	worker = Worker{
		id:   coordinator.next_worker_id,
		task: task,
	}
	// if !coordinator.finish_map_reduce {
	//log.Print("Assigned task ", task.task_id, " - type task: ", task.task_type)
	// }

	coordinator.workers = append(coordinator.workers, worker)
	if coordinator.finish_map_reduce {
		log.Print("No more tasks")
		defer coordinator.server.Stop()
	}
	return &protos.GiveTask{
		TypeTask: int32(task.task_type),
		Files:    task.files,
		TaskId:   task.task_id,
		WorkerId: coordinator.next_worker_id,
		Reducers: config.Reducers,
	}, nil
}

func fetch_task() *Task {
	var task *Task
	task = fetch_map_task()
	if task == nil {
		task = fetch_reduce_task()
	}
	if task == nil {
		task = &Task{task_type: config.Finish, status: config.Available}
		coordinator.finish_map_reduce = true
	}
	return task
}

func fetch_map_task() *Task {

	// If MAP task available
	for i := 0; i < len(coordinator.map_tasks); i++ {
		task := &coordinator.map_tasks[i]
		if task.status == config.Available {
			task.status = config.Unavailable
			task.time = time.Now()
			return task
		}
	}

	//If MAP task not available, see if one is taking too long
	for i := 0; i < len(coordinator.map_tasks); i++ {
		task := &coordinator.map_tasks[i]
		if task.status == config.Unavailable && (time.Since(task.time)).Seconds() >= config.MaxTimeSeconds {
			task.time = time.Now()
			return task
		}
	}

	//If all MAP tasks are taken and on time, assign the first one unavailable
	for i := 0; i < len(coordinator.map_tasks); i++ {
		task := &coordinator.map_tasks[i]
		if task.status != config.Completed {
			task.time = time.Now()
			return task
		}
	}

	return nil
}

func fetch_reduce_task() *Task {

	// If REDUCE task available
	for id, task := range coordinator.reduce_tasks {
		if task.status == 0 {
			task.status = 1
			task.time = time.Now()
			coordinator.reduce_tasks[id] = task
			log.Print("tomada Task desocupada")
			log.Print("Assigned REDUCE task ", task.task_id, " - file: ", task.files)
			return &task
		}
	}

	//If REDUCE task not available, see if one is taking too long
	for id, task := range coordinator.reduce_tasks {
		if task.status == 1 && (time.Since(task.time)).Seconds() >= config.MaxTimeSeconds {
			log.Print("Reduced task has reached time limit")
			log.Print("tomada Task demorada")
			task.time = time.Now()
			coordinator.reduce_tasks[id] = task
			return &task
		}
	}

	return nil
}

func get_reducer_ID(file string) int {
	base := strings.TrimSuffix(file, ".txt")
	parts := strings.Split(base, "-")
	reducerStr := parts[len(parts)-1]
	reducerID, err := strconv.Atoi(reducerStr)
	if err != nil {
		log.Fatalf("Invalid reducer id in filename %s: %v", file, err)
	}
	return reducerID
}

func assign_partial_results(file_names []string) {
	for _, file := range file_names {
		reducer_id := get_reducer_ID(file)
		task := coordinator.reduce_tasks[int32(reducer_id)]
		task.files = append(task.files, file)
		coordinator.reduce_tasks[int32(reducer_id)] = task
	}
}

func (s *myCoordinatorServer) FinishedTask(_ context.Context, result *protos.TaskResult) (*protos.Ack, error) {
	var worker_position int32
	var worker_task *Task
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()

	// CUIDADO CON USAR WORKER ID COMO TASK IDS
	var task_id int32
	for i := int32(0); i < int32(len(coordinator.workers)); i++ {
		task_id = coordinator.workers[i].id
		if task_id == result.WorkerId {
			worker_position = i
			worker_task = coordinator.workers[i].task
			break
		}
	}
	coordinator.workers = append(coordinator.workers[:worker_position], coordinator.workers[worker_position+1:]...)

	if worker_task.status == config.Completed {
		return &protos.Ack{CommitState: int32(config.Deny)}, nil
	}
	worker_task.status = config.Completed

	if worker_task.task_type == config.Map {
		assign_partial_results(result.FileNames)
	} else if worker_task.task_type == config.Reduce {
		task := coordinator.reduce_tasks[task_id]
		task.status = config.Completed
		coordinator.reduce_tasks[task_id] = task
	}
	return &protos.Ack{CommitState: int32(config.Accept)}, nil
}

func start_server() {
	if _, err := os.Stat(socketPath); err == nil {
		os.Remove(socketPath)
	}

	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := coordinator.server
	service := &myCoordinatorServer{}
	protos.RegisterCoordinatorServer(server, service)
	log.Printf("Coordinator listening on unix socket %s", socketPath)
	err = server.Serve(lis)
	if err != nil {
		log.Fatalf("cant serve: %s", err)
	}
	os.Remove(socketPath)
}

const socketPath = "/tmp/mapreduce.sock"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: go run coordinator.go file1.txt file2.txt ...\n")
		os.Exit(1)
	}

	files := os.Args[1:]
	init_coordinator(files)

	start_server()

	log.Printf("Exit")
}
