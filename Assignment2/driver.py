#This script will spawn all the processes
import json
import multiprocessing
from myProcess import MyProcess
import os

def handle_process(object):
    process = MyProcess(object)
    print(f"A process with pid {object['pid']} was created")

if __name__ =="__main__":
    file_path = './process_mapping.json'

    try:
        with open(file_path, 'r') as file:
            data = json.load(file)

    except Exception as e:
        print(e)

    for obj in data:
        #spawn the processes
        process = multiprocessing.Process(target=handle_process, args=(obj, ))
        process.start()
        