class Message():

    def __init__(self, pid, data_block, clock):
        self.pid = pid
        self.data_block = data_block
        self.clock = clock


    def serialize(self):
        return str(self.pid) + " " + str(self.data_block) + " " + str(self.clock)
    

    @staticmethod
    def deserialize(message):
        pid, data_block, clock = message.split(" ")
        return Message(int(pid), data_block, int(clock))
    
