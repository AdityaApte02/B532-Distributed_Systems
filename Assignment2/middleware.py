import socket
import threading
import heapq
import time
import csv
from Message import Message

class Middleware():
    def __init__(self, pid, Middleware_HostName, Middleware_Application_Receive_port, Middleware_Network_Receive_port, Application_Hostname, Application_Middleware_Receive_port):
        self.pid = pid
        self.host = Middleware_HostName
        self.Middleware_Application_Receive_port = Middleware_Application_Receive_port
        self.Middleware_Network_Receive_port = Middleware_Network_Receive_port
        self.Application_Hostname = Application_Hostname
        self.Application_Middleware_Receive_port = Application_Middleware_Receive_port
        self.network_ports = {}
        self.queue = []
        self.acks = 0
        self.clock = int(pid)
        self.read_network_ports()
        self.run()
        


    def read_network_ports(self):
        with open('./network_receive.csv', 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                self.network_ports[row[0]] = int(row[1])

    def receiveFromApplication(self):
        from_application_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            from_application_socket.bind((self.host, self.Middleware_Application_Receive_port))
            from_application_socket.listen(1)
            while True:
                conn, addr = from_application_socket.accept()
                data = conn.recv(1024)
                if not data:
                    break

                self.clock = self.clock + 1
                message = data.decode("utf-8")
                messageObj = Message(message[1], message[0], self.clock)
                self.sendToNetwork(messageObj.serialize())

        except Exception as e:
            print(e)


    def sendToApplication(self):
        while True:
            time.sleep(10)
            try:
                to_application_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                to_application_socket.connect((self.Application_Hostname, self.Application_Middleware_Receive_port))
                data = "Hello from Middleware!!"
                to_application_socket.send(data.encode("utf-8"))
            except Exception as e:
                print("Error while sending data", e)
            finally:
                to_application_socket.close()


    def receiveFromNetwork(self):
        from_network_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            from_network_socket.bind((self.host, self.Middleware_Network_Receive_port))
            from_network_socket.listen(1)
            while True:
                conn, address = from_network_socket.accept()
                data = conn.recv(1024)
                messageObj = Message.deserialize(data.decode('utf-8'))
                self.clock = max(self.clock, messageObj.clock) + 1
                if not data:
                    break
                print(f"From network {data.decode('utf-8')} at {self.pid}")
                print(f"Time of process {self.pid} is {self.clock}")
        except Exception as e:
            print(e)

    def sendToNetwork(self, message_from_application):
        for key, value in self.network_ports.items():
            try:
                to_network_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                to_network_socket.connect(("localhost", value))
                data = message_from_application
                to_network_socket.send(data.encode("utf-8"))
            except Exception as e:
                print(e)
            finally:
                to_network_socket.close()

    def run(self):
        receive_application = threading.Thread(target=self.receiveFromApplication)
        receive_network = threading.Thread(target=self.receiveFromNetwork)

        receive_application.start()
        receive_network.start()

        receive_application.join()
        receive_network.join()