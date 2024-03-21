import threading
import time
import socket
import os
from message import PulseMessageReducer

class Reducer():
    def __init__(self, masterHost, masterPort, id, host, port, reduce_func):
        self.masterHost = masterHost
        self.masterPort = masterPort
        self.id = id
        self.host = host
        self.port = port
        self.reduce_func = reduce_func
        self.PULSE_INTERVAL = 5
        self.reduce_path = os.path.join(os.getcwd(), self.reduce_func)
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
                print("msg_list",msg_list)
        except Exception as e:
            print(e)
        finally:
            mapper_socket.close()
        
    def run(self):
        thread= threading.Thread(target=self.sendPulseToMaster, args=())
        thread.start()
        
        listenThread = threading.Thread(target=self.receiveFromMapper,args=())
        listenThread.start()