import multiprocessing
from mapper import Mapper
import socket
import threading
import time
from reducer import Reducer

class Master():
    def __init__(self, host, port, mappers, reducers, map_function, reduce_function):
        self.host = host
        self.port = port
        self.num_mappers = len(mappers)
        self.num_reducers = len(reducers)
        self.mappers = mappers
        self.reducers = reducers
        self.map_function = map_function
        self.reduce_function = reduce_function
        self.pulse_times = {}
        self.track_mappers = {}
        self.mappersDone = False
        self.TIMEOUT = 4
        
        
    def handle_mappers(self, mapper_obj):
        mapper = Mapper(self.host, self.port, mapper_obj["id"], mapper_obj["port"], self.map_function)
        print(f'spawned mapper with id {mapper_obj["id"]}')
        
    def start_mappers(self):
        mapper_processes = []
        for i in range(len(self.mappers)):
            process = multiprocessing.Process(target=self.handle_mappers, args=(self.mappers[i], ))
            process.start()
            mapper_processes.append(process)
            
        return mapper_processes
            
    def handle_reducers(self):
        reducer = Red
            
    def start_reducers(self):
        reducer_processes = []
        for i in range(len(self.reducers)):
            process = multiprocessing.Process(target=self.handle_reducers, args=(self.reducers[i,]))
            process.start()
            reducer_processes.append(process)
            
        return reducer_processes
            
    def listenFromMapper(self):
        mapper_pulse_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            mapper_pulse_socket.bind((self.host, self.port))
            mapper_pulse_socket.listen(1)
            while True:
                conn, address = mapper_pulse_socket.accept()
                data = conn.recv(1024)
                if not data:
                    break
                data = data.decode('utf-8')
                msg_str_list = data.split(' ')
                if msg_str_list[0] == "Pulse":
                    mapper_id = msg_str_list[1]
                    self.pulse_times[mapper_id] = time.time()
                    
                elif msg_str_list[0] == "Done_M":
                    mapper_id = msg_str_list[1]
                    self.track_mappers[mapper_id] = True
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
                
            
    def checkPulse(self):
        while True:
            current_time = time.time()
            for mapper_id, last_pulse_time in self.pulse_times.items():
                if current_time - last_pulse_time > self.TIMEOUT:
                    print(f"Terminating Mapper with id {mapper_id}")
                    self.killMapper()  
                else:
                    print('Mapper is still alive')
                    
            time.sleep(1)
            
            
    def killMapper(self):
        '''
        Terminate the mapper and restart it
        '''
        
    
    def start_reducers(self):
        pass
        
    def run(self):
        for mapper in self.mappers:
            self.track_mappers[mapper["id"]] = False
            
        self.start_mappers()
        time.sleep(2)
        
        map_listen = threading.Thread(target=self.listenFromMapper, args=())
        map_listen.start()
        
        check_pulse = threading.Thread(target=self.checkPulse, args=())
        check_pulse.start()
        
        check_mappers = threading.Thread(target=self.checkMappers, args=())
        check_mappers.start()
        