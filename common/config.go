package config

const SocketPath = "/tmp/mapreduce.sock"

const MaxTimeSeconds = 3

const Reducers = 2

type TaskType int32

const (
	Map    TaskType = 0
	Reduce TaskType = 1
	Finish TaskType = 2
	Wait   TaskType = 3
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

const (
	Parcial_path = "filesystem/parcial_result/"
	Result_path  = "filesystem/final_result/"
)
