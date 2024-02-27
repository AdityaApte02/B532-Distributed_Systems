class Message():
    def __init__(self, pid, data_block, clock):
        self.pid = pid
        self.data_block = data_block
        self.clock = clock
        self.acks = 0
        self.hash = hash(str(self.pid) + str(self.data_block) + str(self.clock))

    def __lt__(self, other):
        return self.clock < other.clock

    def serialize(self):
        return "MSG " + str(self.pid) + " " + str(self.data_block) + " " + str(self.clock)
    

    @staticmethod
    def deserialize(message):
        pid, data_block, clock = message[4:].split(" ")
        return Message(int(pid), data_block, int(clock))
    
