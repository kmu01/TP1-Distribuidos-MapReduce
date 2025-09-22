
import subprocess
from pathlib import Path
import glob
from collections import defaultdict
from time import sleep

def limpiar_fs():
    subprocess.run("bash empty_fs.sh || true", shell=True)

def crear_archivos_pg():
    base_dir = Path("filesystem/pg")
    base_dir.mkdir(parents=True, exist_ok=True)
    f1_path = base_dir / "pg-1.txt"
    f2_path = base_dir / "pg-2.txt"
    with open(f1_path, "w") as f1:
        f1.write("hola don pepito\n")
    with open(f2_path, "w") as f2:
        f2.write("hola don jose\n")

def get_all_reduces_wc():
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

def compare_wc(reduces_dict):
    with open("filesystem/final_result/sequential-out.txt", "r") as f:
        for line in f:
            parts = line.split()
            if parts[0] not in reduces_dict or int(reduces_dict[parts[0]]) != int(parts[1]):
                print(f"[Differ] {parts} != {reduces_dict}")
                return False
        return True

def check_worker_log():
    for log_file in glob.glob("logs/worker_*.log"):
        with open(log_file) as f:
            if "Worker Failed. EXIT" in f.read():
                return True
    return False

def test_failure_reassign():
    limpiar_fs()
    crear_archivos_pg()
    subprocess.run(
        "go run cmd/seq/mainseq.go plugins/wc.so filesystem/pg/pg-*.txt",
        shell=True,
        check=True,
    )
    # Correr concurrentemente
    subprocess.run(["./run_mr.sh 10 wc 25"], shell=True, check=True)
    reduces = get_all_reduces_wc()
    assert compare_wc(reduces), "Results differ"
    assert check_worker_log(), "No se detectó fallo de worker en los logs"
    print("[TEST FAILURE REASSIGN] OK ☺️")

if __name__ == "__main__":
    test_failure_reassign()
