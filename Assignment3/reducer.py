import threading
import time
import sys
import socket
import os
from message import PulseMessageReducer
from message import DoneMessageReducer
import subprocess
import signal

class Reducer():
    def __init__(self, masterHost, masterPort, id, host, port, reduce_func, num_mappers, test):
        self.testCase = test
        self.masterHost = masterHost
        self.masterPort = masterPort
        self.id = id
        self.host = host
        self.port = port
        self.reduce_func = reduce_func
        self.PULSE_INTERVAL = 5
        self.num_mappers = num_mappers
        self.input_path = os.path.join(os.getcwd(), f"tests/{self.testCase}/home","reducers",str(self.id),"input.txt")
        self.output_path = os.path.join(os.getcwd(), f"tests/{self.testCase}/home","reducers",str(self.id),"output.txt")
        self.reduce_path = os.path.join(os.getcwd(), f"tests/{self.testCase}", self.reduce_func)
        self.mapper_dict = {}
        self.end = False
        self.run()
        
    def sendPulseToMaster(self):
        '''
        Send the pulse signal to the Master
        '''
        while not self.end:
            if self.end:
                break
            time.sleep(self.PULSE_INTERVAL)
            pulse_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            pulse_socket.connect((self.masterHost, self.masterPort))
            try:
                pulseMessageObj = PulseMessageReducer(self.id)
                msg = pulseMessageObj.serialize()
                pulse_socket.send(msg.encode("utf-8")) 
            except Exception as e:
                print(e)
            finally:
                pulse_socket.close()
                
                
    def listen(self):
        mapper_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            mapper_socket.bind((self.host, self.port))
            mapper_socket.listen(10)
            while True:
                if self.end:
                    break
                conn, address = mapper_socket.accept()
                data = conn.recv(1024)
                if not data:
                    break
                data = data.decode("utf-8")
                msg_list = data.split(" ")
                mapper_id = msg_list[1]
                msg = msg_list[0]
                if msg == "DONE_MAPPER_REDUCER":
                    self.mapper_dict[mapper_id] = True  
                elif msg == "SEND_REDUCER":
                    key = msg_list[2]
                    value = msg_list[3]
                    with open(self.input_path , 'a') as file:
                        file.write(key+'\t'+value+'\n')
                elif msg == "TERMINATE":
                    self.terminate()
        except Exception as e:
            print(e)
        finally:
            mapper_socket.close()
            
    def execute(self):
        command = ["python", self.reduce_path, "<", self.input_path, ">", self.output_path]

        process = subprocess.Popen(command, shell=True)
        return_code = process.wait()
            
    
    def terminate(self):
        print('Terminating Reducer ', self.id)
        self.end = True
        os.kill(os.getpid(), signal.SIGINT)
    
    def sendDoneToMaster(self):
        time.sleep(1)
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.connect((self.masterHost, self.masterPort))
        try:
            send_master_obj = DoneMessageReducer(self.id)
            msg = send_master_obj.serialize()
            master_socket.send(msg.encode("utf-8")) 
        except Exception as e:
            print(e)
        finally:
            master_socket.close()
                
    
    def checkMappers(self):
        if self.end:
            return
        flag = True
        for mapper in self.mapper_dict.keys():
            if self.mapper_dict[mapper] == False:
                flag = False
                break
            
        if flag:
            print('Start Reducing')
            time.sleep(3)
            self.execute()
            time.sleep(1)
            self.sendDoneToMaster()
                
        
    def run(self):
        thread= threading.Thread(target=self.sendPulseToMaster, args=())
        thread.start()
        
        listenThread = threading.Thread(target=self.listen,args=())
        listenThread.start()
        
        checkMapperThread = threading.Thread(target=self.checkMappers, args=())
        checkMapperThread.start()
        
        