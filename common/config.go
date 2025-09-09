package config

const socketPath = "/tmp/mapreduce.sock"

const MaxTimeSeconds = 10

const Reducers = 2

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
