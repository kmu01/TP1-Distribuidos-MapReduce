## Commands to run

### How to build the service

Build the plugins

```bash
go build -buildmode=plugin -o plugins/ii.so plugins/ii/ii.go
```

Build the protos

```bash
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative  common/protos/communication.protos
```

### How to run the service

Run the coordinator

```bash
go run cmd/coordinator/coordinator.go filesystem/pg/pg-*.txt
```

A single worker with the `p` plugin and a `n`% probability of failure

```bash
go run ./cmd/worker/worker.go ./plugins/p.so <n>
```

Coordinator with multiple workers (see the bash file to set the plugin, the number of workers and the failure probability)

```bash
./run_mr.sh
```
