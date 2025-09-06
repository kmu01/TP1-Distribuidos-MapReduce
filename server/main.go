package main

import (
	"context"
	"log"
	"mapreduce-tp/mapreduce/protos"
	"net"

	"google.golang.org/grpc"
)

type myCoordinatorServer struct {
	protos.UnimplementedCoordinatorServer
}

type Coordinator struct {
	workers []Worker ///workers que estan activos
	tasks   []Task   ///completed and not completed
	/// files to read
	/// diccionario con workers y los files que se estan mapeando?
	/// lista de resultados de map para pasar a reduce?
}

type Task struct {
	task_type int32 //map/reduce/nothing
	status    int32 //disponible/tomada/completada
	worker_id int32
}

type Worker struct {
	id        int32
	task      *Task
	status    int32
	timestamp int32 /// momento en el que le asignamos la tarea
}

var coordinator = Coordinator{}

func CoordinatorInit() {
	// inicializamos coordinador
	task := Task{task_type: 1, status: 0, worker_id: -1} /// id negativo cuando no hay worker id?
	coordinator.tasks = append(coordinator.tasks, task)
}

// respondemos al RequestTask del worker
func (s *myCoordinatorServer) AssignTask(ctx context.Context, in *protos.RequestTask) (*protos.GiveTask, error) {
	log.Print("[CLIENT] Assign Task")

	// if coordinator libros para asignar > 0 map else reduce
	var worker Worker

	var task *Task
	task = fetchTask(0, in.WorkerId)
	if task == nil {
		task = fetchTask(1, in.WorkerId)
	}
	if task == nil {
		task = &Task{task_type: 2, status: 0, worker_id: in.WorkerId}
	}

	worker = Worker{
		id:        in.WorkerId,
		task:      task,
		status:    1,
		timestamp: 1,
	}

	coordinator.workers = append(coordinator.workers, worker)

	return &protos.GiveTask{TypeTask: 0}, nil
}

func fetchTask(task_type int32, worker_id int32) *Task {
	for i := 0; i < len(coordinator.tasks); i++ {
		task := &coordinator.tasks[i]
		if task.task_type == task_type && task.status == 0 {
			task.status = 1
			task.worker_id = worker_id
			return task
		}
	}
	return nil
}

func (s *myCoordinatorServer) FinishedTask(_ context.Context, in *protos.TaskResult) (*protos.Ack, error) {
	for i := 0; i < len(coordinator.workers); i++ {
		task_id := coordinator.workers[i].id
		if task_id == in.WorkerId {
			coordinator.workers[i].task.status = 1
			log.Print("[CLIENT] Finished Task: ", task_id)
		}
	}

	return &protos.Ack{Ack: "ack"}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":8090")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	CoordinatorInit()

	server := grpc.NewServer()
	service := &myCoordinatorServer{}
	protos.RegisterCoordinatorServer(server, service)
	err = server.Serve(lis)
	if err != nil {
		log.Fatalf("cant serve: %s", err)
	}
}
