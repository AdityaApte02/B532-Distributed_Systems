import socket
import threading
import heapq
import time
import csv
from Message import Message
from acknowldegement import Acknowledgement

class Middleware():
    def __init__(self, num_processes, network_ports_path, pid, Middleware_HostName, Middleware_Application_Receive_port, Middleware_Network_Receive_port, Application_Hostname, Application_Middleware_Receive_port):
        self.num_processes = num_processes
        self.pid = pid
        self.host = Middleware_HostName
        self.Middleware_Application_Receive_port = Middleware_Application_Receive_port
        self.Middleware_Network_Receive_port = Middleware_Network_Receive_port
        self.Application_Hostname = Application_Hostname
        self.Application_Middleware_Receive_port = Application_Middleware_Receive_port
        self.network_ports = {}
        self.network_ports_path = network_ports_path
        self.queue = []
        self.ack_list = []
        self.ack_dict = {}
        self.clock = int(pid)
        self.read_network_ports()
        self.run()
        
    def processQueue(self):
        # Actual Total Order Broadcast Algorithm
        while True:
            if len(self.queue) > 0:
                if self.queue[0].acks >= self.num_processes:
                    self.sendToApplication(self.queue[0].serialize())
                    heapq.heappop(self.queue)
                else:
                    if self.queue[0].hash not in self.ack_dict.keys():
                        time.sleep(1)
                        ackObject = Acknowledgement(self.queue[0].pid, self.queue[0].data_block, self.queue[0].clock)
                        self.sendToNetwork(ackObject.serialize())
                        self.ack_dict[ackObject.hash] = True
            

    def read_network_ports(self):
        with open(self.network_ports_path, 'r') as file:
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
                print('Received a message from Application', messageObj.serialize())
                self.sendToNetwork(messageObj.serialize())

        except Exception as e:
            print(e)


    def sendToApplication(self, data):
        try:
            to_application_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            to_application_socket.connect((self.Application_Hostname, self.Application_Middleware_Receive_port))
            to_application_socket.send(data.encode("utf-8"))
        except Exception as e:
            print("Error while sending data", e)
        finally:
            to_application_socket.close()

    def processAcks(self):
        for messageObj in self.queue:
            for i in range(len(self.ack_list)):
                ackObj = self.ack_list[i]
                if messageObj.hash == ackObj.hash:
                    messageObj.acks += 1
                    self.ack_list.pop(i)
                    break


    def receiveFromNetwork(self):
        from_network_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            from_network_socket.bind((self.host, self.Middleware_Network_Receive_port))
            from_network_socket.listen(1)
            while True:
                conn, address = from_network_socket.accept()
                data = conn.recv(1024)
                if not data:
                    break
                data = data.decode('utf-8')
                if data[:3] == 'MSG':
                    messageObj = Message.deserialize(data)

                    #Update the clock by taking max of your timestamp and timestamp of received messgage and incrementing it by 1
                    self.clock = max(self.clock, messageObj.clock) + 1   
                    
                    heapq.heapify(self.queue)
                    heapq.heappush(self.queue, messageObj)
                else:
                    ackObj = Acknowledgement.deserialize(data)
                    self.ack_list.append(ackObj)
                    self.processAcks()


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
        process_queue = threading.Thread(target=self.processQueue)

        receive_application.start()
        receive_network.start()
        process_queue.start()

        receive_application.join()
        receive_network.join()
        process_queue.join()