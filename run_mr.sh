#!/bin/bash
# Script para levantar workers de MapReduce junto con su coordinator
# Uso: ./run_mr.sh <N> <plugin> <P>
# Ejemplo: ./run_mr.sh 5 wc 0.5

N=${1:-3}  # Número de workers, por defecto 3 si no se especifica
PLUGIN=${2:-wc}  # Plugin a usar, por defecto wc
FAILURE_PROB=${3:-0} #probabilidad de falla de worker, por defecto 0


# Crear logs solo si no existe
mkdir -p logs

if [ -z "$N" ] || [ -z "$PLUGIN" ]; then
    echo "Uso: $0 <N> <plugin>"
    exit 1
fi

echo "[Coordinator] Iniciando coordinator..."
go run cmd/coordinator/coordinator.go filesystem/pg/pg-*.txt > logs/coordinator.log 2>&1 &
COORD_PID=$!


SOCKET_PATH="/tmp/mapreduce.sock" 
echo "Esperando a que el coordinator esté listo..."
for i in {1..40}; do
    if [ -S "$SOCKET_PATH" ]; then
        break
    fi
    sleep 0.5
done

WORKER_PIDS=()
for i in $(seq 1 $N); do
    echo "[Worker $i] Iniciando worker con plugin $PLUGIN..."
    go run cmd/worker/worker.go plugins/$PLUGIN.so $FAILURE_PROB > logs/worker_$i.log 2>&1 &
    WORKER_PIDS+=($!)
done


# Esperar a que todos los procesos terminen
wait $COORD_PID
for pid in "${WORKER_PIDS[@]}"; do
    wait $pid 2>/dev/null
done

echo "Se lanzo el algoritmo de map reduce en segundo plano"
echo "Revisar los logs en logs/"