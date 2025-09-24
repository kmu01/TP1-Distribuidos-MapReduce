## Commands to run

### How to build the service

Build the plugins

```bash
go build -buildmode=plugin -o plugins/ii.so plugins/ii/ii.go
go build -buildmode=plugin -o plugins/wc.so plugins/wc/wc.go
```

Build the protos

```bash
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative  common/protos/communication.protos
```

### How to run the service

Run everything together with `N` workers, plugin `plugin` and `P` probability of failure

```bash
./run_mr.sh <N> <plugin> <P>
```

Run the coordinator

```bash
go run cmd/coordinator/coordinator.go filesystem/pg/pg-*.txt
```

A single worker with the `p` plugin and a `n`% probability of failure

```bash
go run ./cmd/worker/worker.go ./plugins/p.so <n>
```

To run sequentially

```bash
go run cmd/seq/mainseq.go plugins/wc.so filesystem/pg/pg-*.txt
```

### How to test

Either word count or inverted index

```bash
python3 ./tests/test_wc.py
python3 ./tests/test_ii.py
```
