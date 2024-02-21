import csv
import time
class Operation():
    @staticmethod
    def parseCommand(client_socket, command):
        fields = command
        request = fields[0]
        if request == 'set':
            chunkSize = fields[2]
            if fields[3] == "noreply":
                value = fields[4]
            else:
                value = fields[3]
        key = fields[1]

        if request == 'set':
            return [request, client_socket, key, chunkSize, value]
        
        elif request == 'get':
            return [request, client_socket, key]
        
    
    @staticmethod
    def setToStore(client_socket_addr, key, chunkSize, value):
        print('In SET')
        time.sleep(3)
        store_path = './store.csv'
        with open(store_path, 'r', newline='') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                if row and row[1] == key:
                    return 'NOT-STORED\r\n'

        with open(store_path, 'a', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([client_socket_addr, key, str(len(value)), value])
        return 'STORED\r\n'

    @staticmethod
    def getFromStore(key):
        time.sleep(10)
        store_path = './store.csv'
        with open(store_path, 'r', newline='') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                if row == []:
                    break
                if row[1] == key:
                    print('KEY FOUND')
                    return ("VALUE" + " "+ row[1] + " "+row[2]+ "\r\n" + row[3]+"\n"+"END"+"\r\n")
            return '\n<NOT FOUND>\r\nEND\r\n'

            





