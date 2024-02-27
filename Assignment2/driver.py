#This script will spawn all the processes
import json
import multiprocessing
from myProcess import MyProcess
import os
import shutil
import time
import unittest
from tests import TestOutputFiles

def handle_process(object, num_processes, network_ports_path):
    process = MyProcess(object, num_processes, network_ports_path)
    print(f"A process with pid {object['pid']} was created")
    
def removeFiles(folder_path):
    try:
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Successfully removed file: {file_path}")
        print(f"Successfully emptied the folder: {folder_path}")
    except FileNotFoundError:
        print(f"Error: Folder not found - {folder_path}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ =="__main__":
    input_folder_path = './TestInputs/'
    for i, subfolder in  enumerate(os.listdir(input_folder_path)):
        subfolder_path = os.path.join(input_folder_path, subfolder)
        if os.path.isdir(subfolder_path):
            process_mapping_path = os.path.join(subfolder_path, "process_mapping.json")
            network_ports_path = os.path.join(subfolder_path, "network_receive.csv")
            if os.path.exists(process_mapping_path):
                try:
                    with open(process_mapping_path, 'r') as json_file:
                        data = json.load(json_file)
                        num_processes = len(data)
                        processes = []
                        for obj in data:
                            process = multiprocessing.Process(target=handle_process, args=(obj, num_processes, network_ports_path))
                            process.start()
                            processes.append(process)

                        time.sleep(45)
                        for process in processes:
                            process.terminate()
                            process.join()


                        outputs_folder = './Outputs/'
                        test_inputs_folder = './TestOutputs/Test'+str(i+1)+'/'
                        txt_files = [file for file in os.listdir(outputs_folder) if file.endswith(".txt")]
                        for file_name in txt_files:
                            source_path = os.path.join(outputs_folder, file_name)
                            destination_path = os.path.join(test_inputs_folder, file_name)
                            try:
                                shutil.copy(source_path, destination_path)
                                print(f"Successfully copied {file_name} to Test1 folder.")
                            except FileNotFoundError:
                                print(f"Error: {file_name} not found in the outputs folder.")
                            except Exception as e:
                                print(f"Error: {e}")

                        removeFiles('./Outputs/')    
                except Exception as e:
                    print(e)

    result = unittest.TextTestRunner().run(unittest.TestLoader().loadTestsFromTestCase(TestOutputFiles))

    if result.wasSuccessful():
        print("All test cases passed.")
    
    else:
        print('Some of the test cases failed')