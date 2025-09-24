import tempfile
import subprocess
from pathlib import Path
import glob
import os

import glob
from pathlib import Path
from collections import defaultdict
from aux_functions import (
    clean_filesystem,
    wait_for_coordinator_log,
    check_worker_log,
)


def get_all_reduces_ii():
    result_dir = Path("filesystem/final_result")
    reduce_files = glob.glob(str(result_dir / "mr-out-*.txt"))

    final_result = defaultdict(str)

    for file in reduce_files:
        with open(file, "r") as f:
            for line in f:
                parts = line.strip().split()
                if not parts:
                    continue
                word = parts[0]
                if not final_result[word]:
                    final_result[word] += parts[1]
                else:
                    final_result[word] += "," + parts[1]

    return dict(final_result)


def compare_ii(reduces_dict) -> bool:
    with open("filesystem/final_result/sequential-out.txt", "r") as f:
        for line in f:
            parts = line.split()
            if (
                parts[0] not in reduces_dict.keys()
                or reduces_dict[parts[0]] != parts[1]
            ):
                print(f"{parts[1]} != {reduces_dict[parts[0]]}")
                return False
        return True


def test_1_ii():
    print("[UNIT TEST] Starting")
    base_dir = Path("filesystem/pg")
    base_dir.mkdir(parents=True, exist_ok=True)

    f1 = tempfile.NamedTemporaryFile(
        mode="w+b", dir=base_dir, delete=False, suffix=".txt", prefix="pg-"
    )
    f1.write(b"hola don pepito")
    f1.flush()

    f2 = tempfile.NamedTemporaryFile(
        mode="w+b", dir=base_dir, delete=False, suffix=".txt", prefix="pg-"
    )
    f2.write(b"hola don jose")
    f2.flush()

    # Correr secuencialmente
    subprocess.run(
        "go run cmd/seq/mainseq.go plugins/ii.so filesystem/pg/pg-*.txt",
        shell=True,
        check=True,
    )

    # Correr concurrentemente
    subprocess.run(["./run_mr.sh 3 ii 0"], shell=True, check=True)

    reduces = get_all_reduces_ii()

    try:
        assert compare_ii(reduces), "Results differ"
        print("[UNIT TEST] OK ☺️")

    finally:
        f1.close()
        f2.close()

        clean_filesystem()


def test_ii_failure_reassign():
    print("[FAILURE TEST] Starting")

    subprocess.run("mkdir -p logs", shell=True, check=False)

    base_dir = Path("filesystem/pg")
    base_dir.mkdir(parents=True, exist_ok=True)

    f1 = tempfile.NamedTemporaryFile(
        mode="w+b", dir=base_dir, delete=False, suffix=".txt", prefix="pg-"
    )
    f1.write(b"hola don pepito")
    f1.flush()

    f2 = tempfile.NamedTemporaryFile(
        mode="w+b", dir=base_dir, delete=False, suffix=".txt", prefix="pg-"
    )
    f2.write(b"hola don jose")
    f2.flush()

    f3 = tempfile.NamedTemporaryFile(
        mode="w+b", dir=base_dir, delete=False, suffix=".txt", prefix="pg-"
    )
    f3.write(b"hola don josefina")
    f3.flush()

    f4 = tempfile.NamedTemporaryFile(
        mode="w+b", dir=base_dir, delete=False, suffix=".txt", prefix="pg-"
    )
    f4.write(b"hola don martin")
    f4.flush()

    # Correr secuencialmente
    subprocess.run(
        "go run cmd/seq/mainseq.go plugins/ii.so filesystem/pg/pg-*.txt",
        shell=True,
        check=True,
    )

    # Coordinador
    coordinator = subprocess.Popen(
        "go run cmd/coordinator/coordinator.go filesystem/pg/pg-*.txt > logs/coordinator.log 2>&1",
        shell=True,
    )

    wait_for_coordinator_log("logs/coordinator.log")

    # Worker que va a caer 100% de las veces
    worker1 = subprocess.Popen(
        "go run ./cmd/worker/worker.go ./plugins/ii.so 100 > logs/worker_1.log 2>&1",
        shell=True,
    )

    # Worker que va a caer 100% de las veces
    worker2 = subprocess.Popen(
        "go run ./cmd/worker/worker.go ./plugins/ii.so 100 > logs/worker_2.log 2>&1",
        shell=True,
    )

    # Worker que no va a fallar
    worker3 = subprocess.Popen(
        "go run ./cmd/worker/worker.go ./plugins/ii.so 0 > logs/worker_3.log 2>&1",
        shell=True,
    )

    coordinator.wait()
    worker1.wait()
    worker2.wait()
    worker3.wait()

    # Chequear que al menos uno falló
    try:
        assert check_worker_log(), "No se detectó fallo de worker en los logs"

        reduces = get_all_reduces_ii()
        assert compare_ii(reduces), (
            "Results differ: el trabajo no se terminó correctamente"
        )

        print("[FAILURE TEST] OK ☺️")
    finally:
        f1.close()
        f2.close()
        f3.close()
        f4.close()

        clean_filesystem()


if __name__ == "__main__":
    test_1_ii()
    test_ii_failure_reassign()
