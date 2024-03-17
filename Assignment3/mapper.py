import threading
import time
import os
import socket
from message import PulseMessage
from message import DoneMessageMapper
import subprocess
from map import map_task

class Mapper():
    def __init__(self, masterHost, masterPort, id, port, map_func):
        self.masterHost = masterHost
        self.masterPort = masterPort
        self.id = id
        self.port = port
        self.map_func = map_func
        self.PULSE_INTERVAL = 2
        self.map_path = "map.py"
        # self.input_path = './home/'+self.id+'/input.txt'
        self.input_path = os.path.join(os.getcwd(), "home",str(self.id),"input.txt")
        # self.output_path = './home/'+self.id+'/output.txt'
        
        self.output_path = os.path.join(os.getcwd(),"home",str(self.id),"output.txt")
        
        
        print(self.input_path)
        print(self.output_path)
        print("path",os.getcwd())
        self.run()
        
        
    def sendPulseToMaster(self):
        '''
        Send the pulse signal to the Master
        '''
        
        while True:
            time.sleep(self.PULSE_INTERVAL)
            pulse_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            pulse_socket.connect((self.masterHost, self.masterPort))
            try:
                pulseMessageObj = PulseMessage(self.id)
                msg = pulseMessageObj.serialize()
                pulse_socket.send(msg.encode("utf-8"))
                
            except Exception as e:
                print(e)
                
            finally:
                pulse_socket.close()
                
                
    def sendDoneToMaster(self):
        '''
        Send a Done message to the master once the mapping task is Done
        '''
       
        done_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        done_socket.connect((self.masterHost, self.masterPort))
        try:
            doneMessageObj = DoneMessageMapper(self.id)
            msg = doneMessageObj.serialize()
            done_socket.send(msg.encode("utf-8"))
        except Exception as e:
            print(e)
            
        finally:
                done_socket.close()
                
    def readInput(self,file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            data = file.read()
            return data
                

    def execute(self):
        '''
        Run the map function
        '''
        command = ["python", self.map_path, "<", self.input_path, ">", self.output_path]

        # Start the subprocess
        process = subprocess.Popen(command, shell=True)
        return_code = process.wait()
        if return_code == 0:
            return True
        else:
            return False
        # output = map_task(self.readInput(self.input_path))
        # with open(self.output_path, 'a') as file:
        #     for key, value in output.items():
        #         file.write(key + " "+ str(value) + "\n")
    
    def sendToReducer(self):
        pass
        
        
        
    def run(self):
        # mapper_dir = self.input_path
        # os.makedirs(mapper_dir, exist_ok=True)
        

        thread= threading.Thread(target=self.sendPulseToMaster, args=())
        thread.start()
        
        self.execute()
        self.sendDoneToMaster()
        
        