package common

// import (
// 	"mapreduce-tp/common/protos"
// 	"sync"
// 	"time"

// 	"google.golang.org/grpc"
// )

// const socketPath = "/tmp/mapreduce.sock"

// type TaskType int32

// const max_time_seconds = 10

// const (
// 	Map    TaskType = 0
// 	Reduce TaskType = 1
// 	Finish TaskType = 2
// )

// type TaskStatus int32

// const (
// 	Available   TaskStatus = 0
// 	Unavailable TaskStatus = 1
// 	Completed   TaskStatus = 2
// )

// type TypeCommit int32

// const (
// 	Accept TypeCommit = 1
// 	Deny   TypeCommit = 2
// )

// type myCoordinatorServer struct {
// 	protos.UnimplementedCoordinatorServer
// }

// type Coordinator struct {
// 	workers     []Worker ///workers que estan activos
// 	map_tasks   []Task   ///completed and not completed
// 	reduce_tasks []Task
// 	finish_map_reduce bool
// 	server *grpc.Server
// 	next_worker_id int32
// 	mu sync.Mutex
// }

// type Task struct {
// 	task_id int32
// 	task_type int32 //map/reduce/nothing
// 	status    int32 //disponible/tomada/completada
// 	file      string
// 	time time.Time
// }

// type Worker struct {
// 	id        int32
// 	task      *Task
// 	status    int32

// }
