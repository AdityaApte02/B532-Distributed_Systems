import os
import sys
import json
import csv
import multiprocessing
import time

def worker(process_name, port):
    time.sleep(2)
    print(f"Process {process_name} is running on port {port}")

if __name__ == "__main__":
    # Specify the CSV file path
    csv_file_path = "process_mapping.csv"

    # Read the CSV file and create a list of tuples (process_name, port)
    with open(csv_file_path, 'r') as file:
        reader = csv.reader(file)
        processes = [(row[0], int(row[1])) for row in reader]

    # Spawn processes
    for process_name, port in processes:
        p = multiprocessing.Process(target=worker, args=(process_name, port))
        p.start()


   

    for process in multiprocessing.active_children():
        process.join()


    print("Testing without join here")