class Acknowledgement():
    def __init__(self, pid, data_block, clock):
        self.pid = pid
        self.data_block = data_block
        self.clock = clock
        self.hash =  hash(str(self.pid) + str(self.data_block) + str(self.clock))

    def serialize(self):
        return "ACK " + str(self.pid) + " " + str(self.data_block) + " " + str(self.clock)
    
    @staticmethod
    def deserialize(message):
        pid, data_block, clock = message[4:].split(" ")
        return Acknowledgement(int(pid), data_block, int(clock))