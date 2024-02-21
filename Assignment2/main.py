import socket
import multiprocessing

def handle_client(connection, address):
    print(f"Accepted connection from {address}")
    
    # Handle communication with the client
    data = connection.recv(1024)
    while data:
        print(f"Received data from {address}: {data.decode('utf-8')}")
        connection.sendall(b"Server received: " + data)
        data = connection.recv(1024)
    
    print(f"Connection from {address} closed.")
    connection.close()

def main():
    host = '127.0.0.1'
    port = 5000
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)

    print(f"Server listening on {host}:{port}")

    try:
        while True:
            # Accept a connection from a client
            client_socket, client_address = server_socket.accept()
            print('Connection Received')
            # Spawn a new process to handle the client
            client_process = multiprocessing.Process(target=handle_client, args=(client_socket, client_address))
            client_process.start()

            # Close the socket in the parent process, as the child process has its own copy
            client_socket.close()
    except KeyboardInterrupt:
        print("Server shutting down.")
    finally:
        server_socket.close()

if __name__ == "__main__":
    main()
