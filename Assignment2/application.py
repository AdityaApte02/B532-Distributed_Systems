import random
import time
import json
import socket
import threading
import string




class Application():
    def __init__(self, pid, host, Application_Middleware_Receive_port, middleware_host, middleware_port):
        self.pid = pid
        self.Application_Hostname = host
        self.Application_Middleware_Receive_port = Application_Middleware_Receive_port
        self.middleware_host = middleware_host
        self.middleware_port = middleware_port
        self.messages  = list(string.ascii_uppercase)
        self.message_counter = 0
        self.run()
        

    def sendRequestToMiddleware(self):
        while True:
            time.sleep(10)
            message = self.messages[self.message_counter] + str(self.pid)
            self.message_counter += 1
            try:
                middleware_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                middleware_socket.connect((self.middleware_host, self.middleware_port))
                data = message
                middleware_socket.send(data.encode("utf-8"))
            except Exception as e:
                print(e)
            finally:
                middleware_socket.close()
        

    def receiveFromMiddleware(self):
        app_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            app_socket.bind((self.Application_Hostname, self.Application_Middleware_Receive_port))
            app_socket.listen(1)
            while True:
                conn, address = app_socket.accept()
                data = conn.recv(1024)
                if not data:
                    break
        except Exception as e:
            print(e)
        finally:
            app_socket.close()


    def run(self):
        send_th = threading.Thread(target= self.sendRequestToMiddleware)
        receive_th = threading.Thread(target=self.receiveFromMiddleware)

        send_th.start()
        receive_th.start()

        send_th.join()
        receive_th.join()
    