import tempfile
import subprocess
from pathlib import Path
import glob
import shutil
from collections import defaultdict
import os
from time import sleep
import time


def clean_filesystem():
    subprocess.run(
        "./empty_fs.sh",
        shell=True,
        check=True,
    )


# Esperando a que el coordinador se ponga a escuchar sobre gRPC
def wait_for_coordinator_log(logfile, timeout=10):
    start = time.time()
    while time.time() - start < timeout:
        if os.path.exists(logfile):
            with open(logfile) as f:
                text = f.read()
                if "listening" in text:
                    return True
        time.sleep(0.2)
    raise TimeoutError("Coordinator no levantó en el tiempo esperado")


def get_all_reduces():
    reduce_files = glob.glob("filesystem/final_result/mr-out-*")

    final_result = defaultdict(int)

    for file in reduce_files:
        with open(file, "r") as f:
            for line in f:
                parts = line.strip().split()
                if not parts:
                    continue
                word = parts[0]
                final_result[word] += int(parts[1])

    return dict(final_result)


def compare(reduces_dict) -> bool:
    with open("filesystem/final_result/sequential-out.txt", "r") as f:
        for line in f:
            parts = line.split()
            if parts[0] not in reduces_dict.keys() or int(
                reduces_dict[parts[0]]
            ) != int(parts[1]):
                print(f"[ERROR] {parts} != {reduces_dict}")
                return False
        return True


def test_1():
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
        "go run cmd/seq/mainseq.go plugins/wc.so filesystem/pg/pg-*.txt",
        shell=True,
        check=True,
    )

    # Correr concurrentemente
    subprocess.run(["./run_mr.sh"], shell=True, check=True)

    reduces = get_all_reduces()

    try:
        assert compare(reduces), "Results differ"
        print("[UNIT TEST] OK ☺️")

    finally:
        f1.close()
        f2.close()

        clean_filesystem()


def test_2():
    print("[UPPER/LOWER TEST] Starting")

    base_dir = Path("filesystem/pg")
    base_dir.mkdir(parents=True, exist_ok=True)

    f1 = tempfile.NamedTemporaryFile(
        mode="w+b", dir=base_dir, delete=False, suffix=".txt", prefix="pg-"
    )
    f1.write(b"HOla dON don senioR seNIoritOO pePItO!")
    f1.flush()

    f2 = tempfile.NamedTemporaryFile(
        mode="w+b", dir=base_dir, delete=False, suffix=".txt", prefix="pg-"
    )
    f2.write(b"HOLA dOn SENIOR JOSE!")
    f2.flush()

    # Correr secuencialmente
    subprocess.run(
        "go run cmd/seq/mainseq.go plugins/wc.so filesystem/pg/pg-*.txt",
        shell=True,
        check=True,
    )

    # Correr concurrentemente
    subprocess.run(["./run_mr.sh"], shell=True, check=True)

    reduces = get_all_reduces()

    try:
        assert compare(reduces), "Results differ"
        print("[UPPER/LOWER TEST] OK ☺️")

    finally:
        f1.close()
        f2.close()

        clean_filesystem()


def test_3():
    print("[PUNCTUATION TEST] Starting")

    base_dir = Path("filesystem/pg")
    base_dir.mkdir(parents=True, exist_ok=True)

    f1 = tempfile.NamedTemporaryFile(
        mode="w+b", dir=base_dir, delete=False, suffix=".txt", prefix="pg-"
    )
    f1.write(b"hola hola chau")
    f1.flush()

    f2 = tempfile.NamedTemporaryFile(
        mode="w+b", dir=base_dir, delete=False, suffix=".txt", prefix="pg-"
    )
    f2.write(b"hola,,, hola!!. chau!$$")
    f2.flush()

    # Correr secuencialmente
    subprocess.run(
        "go run cmd/seq/mainseq.go plugins/wc.so filesystem/pg/pg-*.txt",
        shell=True,
        check=True,
    )

    # Correr concurrentemente
    subprocess.run(["./run_mr.sh"], shell=True, check=True)

    reduces = get_all_reduces()

    try:
        assert compare(reduces), "Results differ"

        print("[PUNCTUATION TEST] OK ☺️")

    finally:
        f1.close()
        f2.close()

        clean_filesystem()


def check_worker_log():
    for log_file in glob.glob("logs/worker_*.log"):
        with open(log_file) as f:
            if "Worker Failed. EXIT" in f.read():
                return True
    return False


def test_wc_failure_reassign():
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
        "go run cmd/seq/mainseq.go plugins/wc.so filesystem/pg/pg-*.txt",
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
        "go run ./cmd/worker/worker.go ./plugins/wc.so 100 > logs/worker_1.log 2>&1",
        shell=True,
    )

    # Worker que va a caer 100% de las veces
    worker2 = subprocess.Popen(
        "go run ./cmd/worker/worker.go ./plugins/wc.so 100 > logs/worker_2.log 2>&1",
        shell=True,
    )

    # Worker que no va a fallar
    worker3 = subprocess.Popen(
        "go run ./cmd/worker/worker.go ./plugins/wc.so 0 > logs/worker_3.log 2>&1",
        shell=True,
    )

    coordinator.wait()
    worker1.wait()
    worker2.wait()
    worker3.wait()

    # Chequear que al menos uno falló
    try:
        assert check_worker_log(), "No se detectó fallo de worker en los logs"

        reduces = get_all_reduces()
        assert compare(reduces), (
            "Results differ: el trabajo no se terminó correctamente"
        )

        print("[FAILURE TEST] OK ☺️")
    finally:
        f1.close()
        f2.close()
        f3.close()
        f4.close()

        clean_filesystem()


def test_stress():
    print("[STRESS TEST] Starting")

    base_dir = Path("filesystem/pg")
    base_dir.mkdir(parents=True, exist_ok=True)

    f1 = tempfile.NamedTemporaryFile(
        mode="w+b", dir=base_dir, delete=False, suffix=".txt", prefix="pg-"
    )
    f1.write(b"hola " * 10000)
    f1.write(b"don " * 2000)
    f1.write(b"pepito " * 5000)
    f1.flush()

    f2 = tempfile.NamedTemporaryFile(
        mode="w+b", dir=base_dir, delete=False, suffix=".txt", prefix="pg-"
    )
    f2.write(b"hola " * 10000)
    f2.write(b"don " * 2000)
    f2.write(b"jose " * 5000)
    f2.flush()

    expected_output = {"hola": 20000, "don": 4000, "pepito": 5000, "jose": 5000}

    # Correr secuencialmente
    subprocess.run(
        "go run cmd/seq/mainseq.go plugins/wc.so filesystem/pg/pg-*.txt",
        shell=True,
        check=True,
    )

    # Correr concurrentemente
    subprocess.run(["./run_mr.sh"], shell=True, check=True)

    reduces = get_all_reduces()

    try:
        assert compare(reduces), "Results differ"
        assert reduces == expected_output, "Results differ"

        print("[STRESS TEST] OK ☺️")

    finally:
        f1.close()
        f2.close()

        clean_filesystem()


if __name__ == "__main__":
    test_1()

    test_2()

    test_3()

    test_wc_failure_reassign()

    test_stress()
