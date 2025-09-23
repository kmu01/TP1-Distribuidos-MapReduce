def check_worker_log():
    for log_file in glob.glob("logs/worker_*.log"):
        with open(log_file) as f:
            if "Worker Failed. EXIT" in f.read():
                return True
    return False

def test_wc_failure_reassign():
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


    # Lanzar workers con alta probabilidad de fallo
    subprocess.run("./run_mr.sh 10 wc 40", shell=True, check=False)

    # Chequear que al menos uno falló
    assert check_worker_log(), "No se detectó fallo de worker en los logs"

    reduces = get_all_reduces()
    assert compare(reduces), "Results differ: el trabajo no se terminó correctamente"

    print("[TEST WC FAILURE REASSIGN] OK ☺️")

    f1.close()
    f2.close()

    subprocess.run(
        "./empty_fs.sh",
        shell=True,
        check=True,
    )
# Caso simple
# Entrada: "hola hola chau"
# Esperado: {"hola": 2, "chau": 1}

# Case-insensitive / normalización (según tu implementación)
# Entrada: "Hola hOlA CHAU"
# Esperado: {"hola": 2, "chau": 1}

# Signos de puntuación
# Entrada: "hola, hola. chau!"
# Esperado: {"hola": 2, "chau": 1}

# Texto vacío
# Entrada: ""
# Esperado: {}

# Archivo grande (stress test)
# Generar texto repetitivo y chequear que el conteo sea correcto.
import tempfile
import subprocess
from pathlib import Path
import glob
import shutil
from collections import defaultdict
import os
from time import sleep


def clean_filesystem():
    result_dir = Path("filesystem/final_result")
    if result_dir.exists():
        shutil.rmtree(result_dir)
    result_dir.mkdir(parents=True, exist_ok=True)


def get_all_reduces():
    print("cwd:", os.getcwd())
    reduce_files = glob.glob("filesystem/final_result/mr-out-*")

    print(f"reduce files: {reduce_files}")
    print("Archivos en final_result:", list(Path("filesystem/final_result").glob("*")))

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
                print(f"[Differ] {parts} != {reduces_dict}")
                return False
        return True


def test_1():
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

    assert compare(reduces), "Results differ"

    print("[TEST UNIT] OK ☺️")

    f1.close()
    f2.close()

    subprocess.run(
        "./empty_fs.sh",
        shell=True,
        check=True,
    )


def test_2():
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

    assert compare(reduces), "Results differ"

    print("[TEST UNIT] OK ☺️")

    f1.close()
    f2.close()

    subprocess.run(
        "./empty_fs.sh",
        shell=True,
        check=True,
    )


def test_3():
    base_dir = Path("filesystem/pg")
    base_dir.mkdir(parents=True, exist_ok=True)

    f1 = tempfile.NamedTemporaryFile(
        mode="w+b", dir=base_dir, delete=False, suffix=".txt", prefix="pg-"
    )
    f1.write(b"hola, hola. chau!")
    f1.flush()

    f2 = tempfile.NamedTemporaryFile(
        mode="w+b", dir=base_dir, delete=False, suffix=".txt", prefix="pg-"
    )
    f2.write(b"hola, hola. chau!")
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

    assert compare(reduces), "Results differ"

    print("[TEST UNIT] OK ☺️")

    f1.close()
    f2.close()

    subprocess.run(
        "./empty_fs.sh",
        shell=True,
        check=True,
    )


if __name__ == "__main__":
    print("Starting Test 1:")
    test_1()

    print("Starting Test 2:")
    test_2()

    print("Starting Test 3:")
    test_3()

    print("Starting Test WC Failure Reassign:")
    test_wc_failure_reassign()
