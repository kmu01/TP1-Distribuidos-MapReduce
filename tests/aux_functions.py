import subprocess
import glob
import os
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


def check_worker_log():
    for log_file in glob.glob("logs/worker_*.log"):
        with open(log_file) as f:
            if "Worker Failed. EXIT" in f.read():
                return True
    return False
