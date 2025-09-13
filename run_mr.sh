#!/bin/bash
# Script para levantar workers de MapReduce junto con su coordinator
# Uso: ./run_mr.sh <N> <plugin> <P>
# Ejemplo: ./run_mr.sh 5 wc 0.5

N=${1:-3}  # Número de workers, por defecto 3 si no se especifica
PLUGIN=${2:-wc}  # Plugin a usar, por defecto wc
FAILURE_PROB=${3:-0} #probabilidad de falla de worker, por defecto 0

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
    go run cmd/worker/worker.go plugins/$PLUGIN.so $FAILURE_PROB > logs/worker_$i.log 2>&1 &
    echo "[Worker $i] Log: worker_$i.log"
done

echo "Se lanzo el algoritmo de map reduce en segundo plano"
echo "Revisar los logs en logs/"