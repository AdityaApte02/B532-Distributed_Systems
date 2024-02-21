import multiprocessing
import socket
import random
import os
import time
import csv

class Application():

    def __init__(self, config_file_path, hostname, port) -> None:
        self.hostname = hostname
        self.port = port
        self.spawn_processes(config_file_path)

    def worker(self, process_name, port):
        print(f"{process_name} is running on port {port}")

    def spawn_processes(self, file_path):
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            processes = [(row[0], int(row[1])) for row in reader]

        # Spawn processes
        for process_name, port in processes:
            p = multiprocessing.Process(target=self.worker, args=(process_name, port))
            p.start()



if __name__ == "__main__":
    application = Application("./process_mapping.csv", "localhost", 8112)
    application_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    application_socket.bind((application.hostname, application.port))
    application_socket.listen(5)
    print("The Application is running!!!")
    try:
        while True:
            process_socket, process_address = application_socket.accept()
            print(f"Process sent a message")

    except Exception as e:
        print(e)

    
