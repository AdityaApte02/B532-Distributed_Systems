import threading
import time
import os
import socket
from message import PulseMessageMapper
from message import DoneMessageMapper
from message import SendToReducerMessage
from message import SendDoneToReducer
import subprocess

class Mapper():
    def __init__(self, masterHost, masterPort, id, host, port, map_func, num_reducers):
        self.masterHost = masterHost
        self.masterPort = masterPort
        self.id = id
        self.host = host
        self.port = port
        self.map_func = map_func
        self.PULSE_INTERVAL = 5
        self.map_path = os.path.join(os.getcwd(), self.map_func)
        self.input_path = os.path.join(os.getcwd(), "home",str(self.id),"input.txt")
        self.output_path = os.path.join(os.getcwd(),"home",str(self.id),"output.txt")
        self.reducers = []
        self.send = False
        self.num_reducers = num_reducers
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
                conn, address = master_socket.accept()
                data = conn.recv(1024)
                if not data:
                    break
                data = data.decode('utf-8')
                msg = data.split(" ")
                reducer = {
                    "id":msg[2],
                    "host":msg[3],
                    "port":int(msg[4])
                }
                self.reducers.append(reducer)
                if len(self.reducers) == self.num_reducers:
                    self.send = True
                    self.sendData()
                
        except Exception as e:
            print(e)
        finally:
            master_socket.close()
          

    def execute(self):
        '''
        Run the map function
        '''
        command = ["python", self.map_path, "<", self.input_path, ">", self.output_path, self.id]

        # Start the subprocess
        process = subprocess.Popen(command, shell=True)
        return_code = process.wait()
      
      
    def getReducerbyId(self, id):
        for reducer in self.reducers:
            if reducer.get('id') == id:
                return reducer
        return None  
      
      
    def sendData(self):
        with open(self.output_path, 'r') as file:
            for line in file:
                if len(line.strip().split('\t')) == 2:
                    word, count = line.strip().split('\t')
                    hash_val = hash(word) % self.num_reducers
                    id = "reducer"+str(hash_val+1)
                    reducer = self.getReducerbyId(id)
                    self.sendToReducer(reducer["host"], reducer["port"], "SendToReducerMessage", word, count)
                    
        time.sleep(2)
        for reducer in self.reducers:
            self.sendToReducer(reducer["host"], reducer["port"], "SendDoneToReducer")
            
                    
        
    def sendToReducer(self, host, port, msgType, key=None, value=None):
        '''
        Apply the hash function and send the data to the reducer.
        '''
        reducer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        reducer_socket.connect((host, port))
        try:
            if msgType == "SendToReducerMessage":
                send_to_reducer_msg = SendToReducerMessage(self.id, key, value)
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
            
                  
    def run(self):
        thread= threading.Thread(target=self.sendPulseToMaster, args=())
        thread.start()
        
        self.execute()
        
        self.sendDoneToMaster()
        listenthread = threading.Thread(target=self.listenFromMaster, args=())
        listenthread.start()
        
    
        
        