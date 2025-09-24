## Map Reduce - Sistemas Distribuidos

- Alejandro Schamun - 108545
- Dolores Levi - 105993
- Federico Camurri - 106359
- Patricio Silva - 106422

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

Folder needed to run manually:

```
|_ filesystem
    |_ pg
    |_ parcial_result
    |_ final_result
```

### How to run the service

Run everything together with `N` workers, plugin `plugin.so` and `P` probability of failure

```bash
./run_mr.sh <N> <plugin.so> <P>
```

Run the coordinator

```bash
go run cmd/coordinator/coordinator.go filesystem/pg/pg-*.txt
```

A single worker with the `plugin.so` plugin and a `n`% probability of failure

```bash
go run ./cmd/worker/worker.go ./plugins/<plugin.so> <n>
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
