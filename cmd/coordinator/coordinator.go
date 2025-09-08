package main

import (
	"context"
	"fmt"
	"log"
	"mapreduce-tp/common/protos"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type TaskType int32

const max_time_seconds = 10

const (
	Map    TaskType = 0
	Reduce TaskType = 1
	Finish TaskType = 2
)

type TaskStatus int32

const (
	Available   TaskStatus = 0
	Unavailable TaskStatus = 1
	Completed   TaskStatus = 2
)

type TypeCommit int32

const (
	Accept TypeCommit = 1
	Deny   TypeCommit = 2
)

type myCoordinatorServer struct {
	protos.UnimplementedCoordinatorServer
}

type Coordinator struct {
	workers     []Worker ///workers que estan activos
	map_tasks   []Task   ///completed and not completed
	reduce_tasks []Task
	finish_map_reduce bool
	server *grpc.Server
	next_worker_id int32
	mu sync.Mutex
}

type Task struct {
	task_id int32
	task_type int32 //map/reduce/nothing
	status    int32 //disponible/tomada/completada
	file      string
	time time.Time
}

type Worker struct {
	id        int32
	task      *Task
	status    int32

}

var coordinator = Coordinator{}

func init_coordinator(files []string) {
	coordinator.server = grpc.NewServer()
	coordinator.next_worker_id = 0
	var id int32 = 0
	for _, filename := range files {
		log.Print("Added new file to coordinator: ", filename)
		new_task := Task{task_type: 0, status: 0, file: filename, task_id: id}
		coordinator.map_tasks = append(coordinator.map_tasks, new_task)
		id+=1
	}
	coordinator.finish_map_reduce = false
}

// respondemos al RequestTask del worker
func (s *myCoordinatorServer) AssignTask(ctx context.Context, in *protos.RequestTask) (*protos.GiveTask, error) {
	coordinator.mu.Lock()
    defer coordinator.mu.Unlock()
	coordinator.next_worker_id += 1
	
	var worker Worker
	var task = fetch_task()
	worker = Worker{
		id:        coordinator.next_worker_id,
		task:      task,
		status:    1,
	}
	if !coordinator.finish_map_reduce {
		//log.Print("Assigned task ", task.task_id, " - type task: ", task.task_type)
	}
	
	coordinator.workers = append(coordinator.workers, worker)
	if coordinator.finish_map_reduce {
		log.Print("No more tasks")
	}
	return &protos.GiveTask{TypeTask: task.task_type, File: task.file, TaskId: task.task_id, WorkerId: coordinator.next_worker_id}, nil
}

func fetch_task() *Task {
	var task *Task
	task = fetch_map_task()
	if task == nil {
		task = fetch_reduce_task()
	}
	if task == nil {
		task = &Task{task_type: 2, status: 0}
		coordinator.finish_map_reduce = true
	}
	return task
}

func fetch_map_task() *Task {
	for i := 0; i < len(coordinator.map_tasks); i++ {
		task := &coordinator.map_tasks[i]
		if task.status == 0 {
			task.status = 1
			task.time = time.Now()
			log.Print("Assigned MAP task ", task.task_id, " - file: ", task.file)
			return task
		}
	}
	for i := 0; i < len(coordinator.map_tasks); i++ {
		task := &coordinator.map_tasks[i]
		if task.status == 1 && (time.Since(task.time)).Seconds() >= max_time_seconds {
			log.Print("Time limit")
			task.time = time.Now()
			return task
		}
	}

	/*
	Si todos los workers ya tomaron sus tareas (status == 1),
	pero no expiró el timeout, este for igual reasigna
	una de esas tareas tomadas a otro worker,
	aunque el primero siga trabajando.

	Ejemplo: 
	2 textos y 3 workers
	Worker 1 toma la tarea 0 (status = 1)
	Worker 2 toma la tarea 1 (status = 1)
	Si ambos siguen trabajando en su tarea Map
	CUando el worker 3 pida tarea, se le reasigna la tarea 0 o 1

	El primer y segundo for no encuentran 
	tareas disponibles(status=0) ni expiradas.

	El tercer for encuentra la tarea 0 (status = 1) status != 2,
	la reasigna y ahora dos workers ejecutan la misma tarea.

	*/
	/*
	for i := 0; i < len(coordinator.map_tasks); i++ {
		task := &coordinator.map_tasks[i]
		if task.status != 2 {
			task.time = time.Now()
			return task
		}
	}
	*/
	return nil
}

func fetch_reduce_task() *Task {
	for i := 0; i < len(coordinator.reduce_tasks); i++ {
		task := &coordinator.reduce_tasks[i]
		if task.status == 0 {
			task.status = 1
			task.time = time.Now()
			log.Print("Assigned REDUCE task ", task.task_id, " - file: ", task.file)
			return task
		}
	}
	for i := 0; i < len(coordinator.reduce_tasks); i++ {
		task := &coordinator.reduce_tasks[i]
		if task.status == 1 && (time.Since(task.time)).Seconds() >= max_time_seconds {
			log.Print("Time limit")
			task.time = time.Now()
			return task
		}
	}

	/*
	for i := 0; i < len(coordinator.reduce_tasks); i++ {
		task := &coordinator.reduce_tasks[i]
		if task.status != 2{
			task.time = time.Now()
			return task
		}
	}
	*/
	return nil
}

func (s *myCoordinatorServer) FinishedTask(_ context.Context, in *protos.TaskResult) (*protos.Ack, error) {
	var worker_position int32
	var worker_task *Task
	coordinator.mu.Lock()
    defer coordinator.mu.Unlock()
	// revisar
	for i := int32(0); i < int32(len(coordinator.workers)); i++ {
		task_id := coordinator.workers[i].id
		if task_id == in.WorkerId {
			worker_position = i
			worker_task = coordinator.workers[i].task
			break
		}
	}
	coordinator.workers = append(coordinator.workers[:worker_position], coordinator.workers[worker_position+1:]...)

	if worker_task.status == 2 {
		return &protos.Ack{CommitState: 2}, nil
	}
	worker_task.status = 2
	if worker_task.task_type == 0 {
		new_task := Task{task_id: worker_task.task_id, task_type: 1, status: 0, file: in.FileName} //crea reduce
		coordinator.reduce_tasks = append(coordinator.reduce_tasks, new_task)
	}
	return &protos.Ack{CommitState: 1}, nil
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
