#!/bin/bash
# Script para levantar N workers de MapReduce junto con su coordinator
# Uso: ./run_workers.sh <N> <plugin>
# Ejemplo: ./run_workers.sh 5 plugins/wc.so

N=${1:-1}  # Número de workers, por defecto 3 si no se especifica
PLUGIN=${2:-plugins/ii.so}  # Plugin a usar, por defecto wc.so
FAILURE_PROB=0

mkdir logs

if [ -z "$N" ] || [ -z "$PLUGIN" ]; then
    echo "Uso: $0 <N> <plugin>"
    exit 1
fi

echo "[Coordinator] Iniciando coordinator..."
go run cmd/coordinator/coordinator.go filesystem/pg/pg-*.txt > logs/coordinator.log 2>&1 &
echo "[Coordinator] Log: coordinator.log"


for i in $(seq 1 $N); do
    echo "[Worker $i] Iniciando worker con plugin $PLUGIN..."
    go run cmd/worker/worker.go $PLUGIN $FAILURE_PROB > logs/worker_$i.log 2>&1 &
    echo "[Worker $i] Log: worker_$i.log"
done

echo "Se lanzo el algoritmo de map reduce en segundo plano"
echo "Revisar los logs en logs/"