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
import os

import glob
from pathlib import Path
from collections import defaultdict


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
            if reduces_dict[parts[0]] != parts[1]:
                print(f"{parts[1]} != {reduces_dict[parts[0]]}")
                return False
        return True


def test_1_ii():
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
        [
            "go run cmd/seq/mainseq.go plugins/ii.so filesystem/pg/pg-*.txt",
        ],
        shell=True,
        check=True,
    )

    # Correr concurrentemente
    subprocess.run(["./run_mr.sh 3 ii 0"], shell=True, check=True)

    reduces = get_all_reduces_ii()

    assert compare_ii(reduces), "Results differ"

    print("[TEST ii 1] OK ☺️")

    f1.close()
    f2.close()


if __name__ == "__main__":
    test_1_ii()
