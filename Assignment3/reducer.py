import threading
import time
import socket
import os
from message import PulseMessageReducer
from message import DoneMessageReducer
import subprocess

class Reducer():
    def __init__(self, masterHost, masterPort, id, host, port, reduce_func, num_mappers):
        self.masterHost = masterHost
        self.masterPort = masterPort
        self.id = id
        self.host = host
        self.port = port
        self.reduce_func = reduce_func
        self.PULSE_INTERVAL = 5
        self.num_mappers = num_mappers
        self.input_path = os.path.join(os.getcwd(), "home",str(self.id),"input.txt")
        self.output_path = os.path.join(os.getcwd(), "home",str(self.id),"output.txt")
        self.reduce_path = os.path.join(os.getcwd(), self.reduce_func)
        self.mapper_dict = {}
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
                pulseMessageObj = PulseMessageReducer(self.id)
                msg = pulseMessageObj.serialize()
                pulse_socket.send(msg.encode("utf-8")) 
            except Exception as e:
                print(e)
            finally:
                pulse_socket.close()
                
                
    def receiveFromMapper(self):
        mapper_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            mapper_socket.bind((self.host, self.port))
            mapper_socket.listen(10)
            while True:
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
                else:
                    key = msg_list[2]
                    value = msg_list[3]
                    print("key",key, " ", "value", value)
                    with open(self.input_path , 'a') as file:
                        file.write(key+'\t'+value+'\n')
        except Exception as e:
            print(e)
        finally:
            mapper_socket.close()
            
    def execute(self):
        command = ["python", self.reduce_path, "<", self.input_path, ">", self.output_path]

        process = subprocess.Popen(command, shell=True)
        return_code = process.wait()
            
            
    def sendDoneToMaster(self):
        while True:
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
        
        listenThread = threading.Thread(target=self.receiveFromMapper,args=())
        listenThread.start()
        
        checkMapperThread = threading.Thread(target=self.checkMappers, args=())
        checkMapperThread.start()