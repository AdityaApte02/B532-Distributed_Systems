import subprocess
import os
import sys

def client_run(path_sc):
    try:
        subprocess.Popen(path_sc)
    except subprocess.CalledProcessError as e:
        print(f"Error running {path_sc}: {e}")

if __name__ == "__main__":
    clients = []

    for input_file in os.listdir("./TestInputs/"):
        clients.append(
            ["python", "./client.py", "./TestInputs/"+ input_file]
        )
        print(input_file)
    for script in clients:
        client_run(script)

