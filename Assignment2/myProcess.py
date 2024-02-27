from application import Application
from middleware import Middleware
import threading


class MyProcess():
    def spawn_Application(self, object, num_processes):
        Application(num_processes, object['pid'], object['Application_HostName'], object['Application_Receive_port'], object['Middleware_HostName'], object['Middleware_Application_Receive_port'])


    def spawn_Middleware(self, object, num_processes, network_ports_path):
        Middleware(num_processes, network_ports_path,  object['pid'], object['Middleware_HostName'], object['Middleware_Application_Receive_port'], object['Middleware_Network_Receive_port'], object['Application_HostName'], object['Application_Receive_port'])


    def __init__(self, object, num_processes, network_ports_path):
        self.pid = object['pid']
        threading.Thread(target=self.spawn_Middleware, args=(object, num_processes, network_ports_path)).start()
        threading.Thread(target=self.spawn_Application, args=(object, num_processes)).start()