import multiprocessing
from mapper import Mapper
import socket
import threading
import sys
import os
import time
import signal
from reducer import Reducer
from message import SendToMapperMessage
from message import Terminate
import processing

class Master():
    def __init__(self, host, port, mappers, reducers, map_function, reduce_function, test, clear):
        self.host = host
        self.port = port
        self.num_mappers = len(mappers)
        self.num_reducers = len(reducers)
        self.mappers = mappers
        self.reducers = reducers
        self.map_function = map_function
        self.reduce_function = reduce_function
        self.mapper_pulse_times = {}
        self.track_mappers = {}
        self.mappersDone = False
        self.reducer_pulse_times = {}
        self.track_reducers = {}
        self.reducersDone = False
        self.end = False
        self.testCase = test
        self.clear = clear
        self.TIMEOUT = 10
        
        
    def handle_mappers(self, mapper_obj):
        mapper = Mapper(self.host, self.port, mapper_obj["id"], mapper_obj["host"], mapper_obj["port"], self.map_function, self.num_reducers, self.testCase)
        print(f'spawned mapper with id {mapper_obj["id"]}')
        
    def start_mappers(self):
        mapper_processes = []
        for i in range(len(self.mappers)):
            process = multiprocessing.Process(target=self.handle_mappers, args=(self.mappers[i], ))
            process.start()
            mapper_processes.append(process)
            
        return mapper_processes
            
    def handle_reducers(self, reducer_obj):
        reducer = Reducer(self.host, self.port, reducer_obj["id"], reducer_obj["host"], reducer_obj["port"], self.reduce_function, self.num_mappers, self.testCase)
        print(f'spawned reducer with id {reducer_obj["id"]}')
            
    def start_reducers(self):
        reducer_processes = []
        for i in range(len(self.reducers)):
            process = multiprocessing.Process(target=self.handle_reducers, args=(self.reducers[i], ))
            process.start()
            reducer_processes.append(process)
            
        return reducer_processes
            
    def listen(self):
        mapper_pulse_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            mapper_pulse_socket.bind((self.host, self.port))
            mapper_pulse_socket.listen(1)
            while True:
                if self.end:
                    break
                conn, address = mapper_pulse_socket.accept()
                data = conn.recv(1024)
                if not data:
                    break
                data = data.decode('utf-8')
                msg_str_list = data.split(' ')
                if msg_str_list[0] == "PULSE_M":
                    mapper_id = msg_str_list[1]
                    self.mapper_pulse_times[mapper_id] = time.time()
                      
                if msg_str_list[0] == "PULSE_R":
                    reducer_id = msg_str_list[1]
                    self.reducer_pulse_times[reducer_id] = time.time()
                    
                elif msg_str_list[0] == "DONE_M":
                    mapper_id = msg_str_list[1]
                    self.track_mappers[mapper_id] = True
                    
                elif msg_str_list[0] == "DONE_R":
                    reducer_id = msg_str_list[1]
                    self.track_reducers[reducer_id] = True
        except Exception as e:
            print(e)
        finally:
            mapper_pulse_socket.close()
                
            
    def checkMappers(self):
        while not self.mappersDone:
            for mapper in self.track_mappers.keys():
                if self.track_mappers[mapper] == True:
                    self.mappersDone = True    
                else:
                    self.mappersDone = False  
            if self.mappersDone:
                print("All mappers are done mapping")
                self.start_reducers()
                time.sleep(1)
                check_pulse_reducers = threading.Thread(target=self.checkPulseReducers, args=())
                check_pulse_reducers.start()
                check_reducers = threading.Thread(target=self.checkReducers,args=())
                check_reducers.start()        
                self.sendToMapper()
        
    def sendToMapper(self):
        '''
        Send a message to mappers to send data to the Reducers
        '''
        print('Sending data to mapper')
        for i in range(len(self.mappers)):
            for j in range(len(self.reducers)):
                send_mapper = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                send_mapper.connect((self.mappers[i]["host"], self.mappers[i]["port"]))
                try:
                    sendtomappermsg = SendToMapperMessage(self.reducers[j])
                    msg = sendtomappermsg.serialize()
                    send_mapper.send(msg.encode("utf-8"))
                except Exception as e:
                    print(e)
                finally:
                    send_mapper.close()
                
    def checkReducers(self):
        while not self.reducersDone:
            for reducer in self.track_reducers.keys():
                if self.track_reducers[reducer] == True:
                    self.reducersDone = True    
                else:
                    self.reducersDone = False  
            if self.reducersDone:
                print("All reducers are done reducing")
                print('Terminating MapReduce')
                self.terminate()
            
    def sendTerminate(self):
        for i in range(len(self.mappers)):
            send_terminate_mapper = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            send_terminate_mapper.connect((self.mappers[i]["host"], self.mappers[i]["port"]))
            try:
                terminate = Terminate()
                msg = terminate.serialize()
                send_terminate_mapper.send(msg.encode("utf-8"))
            except Exception as e:
                print(e)
            finally:
                send_terminate_mapper.close()          
                
        time.sleep(1)
                
        for i in range(len(self.reducers)):
            send_terminate_reducer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            send_terminate_reducer.connect((self.reducers[i]["host"], self.reducers[i]["port"]))
            try:
                terminate = Terminate()
                msg = terminate.serialize()
                send_terminate_reducer.send(msg.encode("utf-8"))
            except Exception as e:
                print(e)
            finally:
                send_terminate_reducer.close()
                
                
    def terminate(self):
        self.sendTerminate()
        time.sleep(3)
        processing.combine(f'tests/{self.testCase}/home/reducers', f'tests/{self.testCase}/combinedOutput.txt')
        time.sleep(2)
        if self.clear == "TRUE":
            processing.cleanUp(self.testCase)
        print('Terminating the Master')
        self.end = True
        time.sleep(4)
        os.kill(os.getpid(), signal.SIGINT)
    
            
    def checkPulseMappers(self):
        while True:
            if self.end:
                break
            current_time = time.time()
            for mapper_id, last_pulse_time in self.mapper_pulse_times.items():
                if current_time - last_pulse_time > self.TIMEOUT:
                    print(f"Terminating Mapper with id {mapper_id}")
                    self.killMapper()  
                else:
                    print('Mapper is still alive')
            time.sleep(1)
            
            
    def checkPulseReducers(self):
        while True:
            if self.end:
                break
            current_time = time.time()
            for reducer_id, last_pulse_time in self.reducer_pulse_times.items():
                if current_time - last_pulse_time > self.TIMEOUT:
                    print(f"Terminating Reducer with id {reducer_id}")
                    self.killReducer()  
                else:
                    print('Reducer is still alive')
            time.sleep(1)
            
            
    def killMapper(self):
        '''
        Terminate the mapper and restart it
        '''
        
    def killReducer(self):
        '''
        Terminate the reducer and restart it
        '''
    
    def run(self):
        print('Running ',self.testCase)
        processing.createFiles(self.testCase, self.num_mappers,self.num_reducers)
        for mapper in self.mappers:
            self.track_mappers[mapper["id"]] = False
            
        self.start_mappers()
        time.sleep(2)
        
        map_listen = threading.Thread(target=self.listen, args=())
        map_listen.start()
        
        check_pulse_mappers = threading.Thread(target=self.checkPulseMappers, args=())
        check_pulse_mappers.start()
        
        check_mappers = threading.Thread(target=self.checkMappers, args=())
        check_mappers.start()
        