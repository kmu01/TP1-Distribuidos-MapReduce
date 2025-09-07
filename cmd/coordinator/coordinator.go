package main

import (
	"context"
	"fmt"
	"log"
	"mapreduce-tp/common/protos"
	"net"
	"os"

	"google.golang.org/grpc"
)

type TaskType int32

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
}

type Task struct {
	task_id int32
	task_type int32 //map/reduce/nothing
	status    int32 //disponible/tomada/completada
	worker_id int32
	file      string
}

type Worker struct {
	id        int32
	task      *Task
	status    int32
	timestamp int32 /// momento en el que le asignamos la tarea
}

var coordinator = Coordinator{}

func init_coordinator(files []string) {
	coordinator.server = grpc.NewServer()
	var id int32 = 0
	for _, filename := range files {
		log.Print("Added new file to coordinator: ", filename)
		new_task := Task{task_type: 0, status: 0, worker_id: -1, file: filename, task_id: id}
		coordinator.map_tasks = append(coordinator.map_tasks, new_task)
		id+=1
	}
	coordinator.finish_map_reduce = false
}

// respondemos al RequestTask del worker
func (s *myCoordinatorServer) AssignTask(ctx context.Context, in *protos.RequestTask) (*protos.GiveTask, error) {
	var worker Worker
	var task = fetch_task(in.WorkerId)
	worker = Worker{
		id:        in.WorkerId,
		task:      task,
		status:    1,
		timestamp: 1,
	}

	coordinator.workers = append(coordinator.workers, worker)
	if coordinator.finish_map_reduce {
		log.Print("No more tasks")
	}
	return &protos.GiveTask{TypeTask: task.task_type, File: task.file, TaskId: task.task_id}, nil
}

func fetch_task(worker_id int32) *Task {
	var task *Task
	task = fetch_map_task(worker_id)
	if task == nil {
		task = fetch_reduce_task(worker_id)
	}
	if task == nil {
		task = &Task{task_type: 2, status: 0, worker_id: worker_id}
		coordinator.finish_map_reduce = true
	}
	return task
}

func fetch_map_task(worker_id int32) *Task {
	for i := 0; i < len(coordinator.map_tasks); i++ {
		task := &coordinator.map_tasks[i]
		if task.status == 0 {
			task.status = 1
			task.worker_id = worker_id
			return task
		}
	}
	//verificar las tareas con mas de 10seg
	return nil
}

func fetch_reduce_task(worker_id int32) *Task {
	for i := 0; i < len(coordinator.reduce_tasks); i++ {
		task := &coordinator.reduce_tasks[i]
		if task.status == 0 {
			task.status = 1
			task.worker_id = worker_id
			return task
		}
	}
	//verificar las tareas con mas de 10seg
	return nil
}

func (s *myCoordinatorServer) FinishedTask(_ context.Context, in *protos.TaskResult) (*protos.Ack, error) {
	var worker_position int32
	var worker_task *Task
	
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

		new_task := Task{task_id: worker_task.task_id, task_type: 1, status: 0, worker_id: -1, file: in.FileName} //crea reduce
		coordinator.reduce_tasks = append(coordinator.reduce_tasks, new_task)
	}
	return &protos.Ack{CommitState: 1}, nil
}

func start_server(porth string) {
	lis, err := net.Listen("tcp", porth)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := coordinator.server
	service := &myCoordinatorServer{}
	protos.RegisterCoordinatorServer(server, service)
	err = server.Serve(lis)
	if err != nil {
		log.Fatalf("cant serve: %s", err)
	}
}

const porth = ":8091"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: go run mainseq.go file1.txt file2.txt ...\n")
		os.Exit(1)
	}

	files := os.Args[1:]
	init_coordinator(files)

	start_server(porth)

	log.Printf("Exit")
}
