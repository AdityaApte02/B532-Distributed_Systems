import threading
import time
import sys
import os
import socket
import random
from message import PulseMessageMapper
from message import DoneMessageMapper
from message import SendToReducerMessage
from message import SendDoneToReducer
from collections import defaultdict
import json
import subprocess
import signal

class Mapper():
    def __init__(self, masterHost, masterPort, id, host, port, map_func, num_reducers, test):
        self.testCase = test
        self.masterHost = masterHost
        self.masterPort = masterPort
        self.id = id
        self.host = host
        self.port = port
        self.map_func = map_func
        self.PULSE_INTERVAL = 1
        self.kill = False
        self.map_path = os.path.join(os.getcwd(), f"tests/{self.testCase}", self.map_func)
        self.input_path = os.path.join(os.getcwd(),"tests",self.testCase,"home","mappers",str(self.id),"input.txt")
        self.output_path = os.path.join(os.getcwd(),"tests",self.testCase,"home","mappers",str(self.id),"output.txt")
        self.reducers = []
        self.send = False
        self.num_reducers = num_reducers
        self.end = False
        self.run()
        
        
    def sendPulseToMaster(self):
        '''
        Send the pulse signal to the Master
        '''
        while True:
            if self.end:
                break
            time.sleep(self.PULSE_INTERVAL)
            pulse_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            pulse_socket.connect((self.masterHost, self.masterPort))
            try:
                pulseMessageObj = PulseMessageMapper(self.id)
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
        time.sleep(1)
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
                
                
    def listenFromMaster(self):
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            master_socket.bind((self.host, self.port))
            master_socket.listen(1)
            while True:
                if self.end:
                    break
                conn, address = master_socket.accept()
                data = conn.recv(1024)
                if not data:
                    break
                data = data.decode('utf-8')
                msg = data.split(" ")
                if msg[0] == "CLEAR_MAPPER":
                    with open(self.output_path, 'w') as file:
                        file.truncate(0)
                    self.execute()
                elif msg[0] == "TERMINATE":
                    self.end = True
                    break
                elif msg[0] == "SEND_MAPPER":
                    reducer = {
                        "id":msg[2],
                        "host":msg[3],
                        "port":int(msg[4])
                    }
                    self.reducers.append(reducer)
                    if len(self.reducers) == self.num_reducers:
                        self.send = True
                        self.sendData()
            self.terminate()
                
        except Exception as e:
            print(e)
        finally:
            master_socket.close()
          
          
    def terminate(self):
        print('Terminating Mapper '+self.id)
        self.end = True
        os.kill(os.getpid(), signal.SIGINT)

    def execute(self):
        '''
        Run the map function
        '''
        command = ["python", self.map_path, "<", self.input_path, ">", self.output_path, self.id]
        process = subprocess.Popen(command, shell=True)
        return_code = process.wait()
      
      
    def getReducerbyId(self, id):
        for reducer in self.reducers:
            if reducer.get('id') == id:
                return reducer
        return None
      
    def computeHash(self,str1):
        return ord(str1[0])

      
    def sendData(self):
        shuffled_data = defaultdict(list)
        with open(self.output_path, 'r') as file:
            for line in file:
                if len(line.strip().split('\t')) == 2:
                    word, count = line.strip().split('\t')
                    shuffled_data[word].append(count)
                    
        sorted_keys = sorted(shuffled_data.keys())
        for key in sorted_keys:
            hash_val = self.computeHash(key) % self.num_reducers
            id = "reducer"+str(hash_val+1)
            reducer = self.getReducerbyId(id)
            self.sendToReducer(reducer["host"], reducer["port"], "SendToReducerMessage", key, shuffled_data[key])
        
        time.sleep(2)
        for reducer in self.reducers:
            self.sendToReducer(reducer["host"], reducer["port"], "SendDoneToReducer")
            
                    
        
    def sendToReducer(self, host, port, msgType, key=None, values=None):
        '''
        Apply the hash function and send the data to the reducer.
        '''
        reducer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        reducer_socket.connect((host, port))
        try:
            if msgType == "SendToReducerMessage":
                send_to_reducer_msg = SendToReducerMessage(self.id, key, values)
                msg = send_to_reducer_msg.serialize()
                reducer_socket.send(msg.encode("utf-8"))      
            elif msgType == "SendDoneToReducer":
                send_done_msg = SendDoneToReducer(self.id)
                msg = send_done_msg.serialize()
                reducer_socket.send(msg.encode("utf-8"))
        except Exception as e:
            print(e)
        finally:
            reducer_socket.close()
            
                    
    def readConfig(self):
        home_dir = f'./tests/{self.testCase}/home/mappers'
        config_path = os.path.join(home_dir, self.id, "config.json")
        with open(config_path, 'r') as file:
            data = json.load(file)
            if data["kill"] == "TRUE":
                self.kill = True
                killThread = threading.Thread(target=self.checkKill, args=())
                killThread.start()
                
                
    def checkKill(self):
        print('in CheckKill')
        if self.kill:
            self.end = True
            path = os.path.join(f"tests/{self.testCase}/home", "mappers",str(self.id), 'config.json')
            with open(path, 'r') as file:
                data = json.load(file)
            data['kill'] = 'FALSE'
            with open(path, 'w') as file:
                json.dump(data, file, indent=4)
            os.kill(os.getpid(), signal.SIGINT)
            
    def createOutputBuffer(self):
        if os.path.exists(self.output_path):
            self.output_path = os.path.join(os.getcwd(),"tests",self.testCase,"home","mappers",str(self.id),"output1.txt")
        with open(self.output_path,"w") as file:
            pass 
            
    def run(self):
        self.createOutputBuffer()
        self.readConfig()
        thread= threading.Thread(target=self.sendPulseToMaster, args=())
        listenthread = threading.Thread(target=self.listenFromMaster, args=())
        thread.start()
        listenthread.start()
        
        self.execute()
        
        self.sendDoneToMaster()
        
    
        
        