#!/bin/bash
# Script para levantar N workers de MapReduce
# Uso: ./run_workers.sh <N> <plugin>
# Ejemplo: ./run_workers.sh 5 plugins/wc.so

N=${1:-3}  # Número de workers, por defecto 3 si no se especifica
PLUGIN=${2:-plugins/wc.so}  # Plugin a usar, por defecto wc.so

if [ -z "$N" ] || [ -z "$PLUGIN" ]; then
    echo "Uso: $0 <N> <plugin>"
    exit 1
fi

for i in $(seq 1 $N); do
    echo "[Worker $i] Iniciando worker con plugin $PLUGIN..."
    go run cmd/worker/worker.go $PLUGIN > worker_$i.log 2>&1 &
    echo "[Worker $i] Log: worker_$i.log"
done

echo "Todos los workers fueron lanzados en segundo plano."
echo "Revisar los logs en worker_*.log"