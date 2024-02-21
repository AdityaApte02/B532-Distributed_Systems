from collections import deque, defaultdict
from socket import *
import threading
from operations import Operation
import os
import csv
import time

locks = defaultdict(threading.Lock)
   

def setHandler(client_socket, key, chunkSize, value):
    lock = locks[key]
    with lock:
        result = Operation.setToStore(client_socket.getsockname()[0], key, chunkSize, value)
    return client_socket.send(bytes(result, "utf-8"))

def getHandler(client_socket, key):
    lock = locks[key]
    with lock:
        return client_socket.send(bytes(Operation.getFromStore(key), "utf-8"))


def threadRequestProvider(requestQueue):
    print("In threadRequestProvider")
    while True:
        while requestQueue:
            print('RequestQueue has requests')
            req = requestQueue.popleft()
            if req[0] == "set":
                with locks[req[2]]:
                    th = threading.Thread(target=setHandler, args=(req[1], req[2], req[3], req[4], ))
                    th.start()
               
            elif req[0] == "get":
                with locks[req[2]]:
                    th = threading.Thread(target=getHandler, args=(req[1], req[2]))
                    th.start()


def handle_client(client_socket, requestQueue):
    try:
        buffer = ''
        while True:
            temp = client_socket.recv(1024).decode('utf-8')
            temp = temp.split()
            if temp == []:
                continue
            if temp[0] == "set" and "noreply" in temp:
                requestQueue.append(Operation.parseCommand(client_socket,temp))
                buffer = ''
            elif temp[0] == "get":
                requestQueue.append(Operation.parseCommand(client_socket,temp))
                buffer = ''
            elif temp[0] == "set":
                buffer = temp
            elif buffer[0] == "set":
                buffer.append(temp[0])
                requestQueue.append(Operation.parseCommand(client_socket,buffer))
                buffer = ""
                
    finally:
        client_socket.close()

host = "127.0.0.1"
port = 8112
server_socket = socket(AF_INET, SOCK_STREAM)

server_socket.bind((host, port))

server_socket.listen()

requestQueue= deque()
thread = threading.Thread(target=threadRequestProvider, args=(requestQueue, ))
thread.start()

print("Server is up and listening!!")

while True:
    client_socket, client_address = server_socket.accept()
    print(f'Accepted connection request from {client_address}')
    client_handler = threading.Thread(target=handle_client, args=(client_socket,requestQueue))
    client_handler.start()
    






