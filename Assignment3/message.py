class Message():
    def __init__(self, messageType, sender):
        self.messageType = messageType
        self.sender = sender
        
    def serialize(self):
        return self.messageType + " " + self.sender
    
    
    @staticmethod
    def deserialize(self, messageType, sender):
        message = Message(messageType, sender)
        return message
        
        
class PulseMessage(Message):
    def __init__(self, sender):
        self.messageType = "Pulse"
        self.sender = sender
        
        
    def serialize(self):
        return super().serialize()
    
    
class DoneMessageMapper(Message):
    def __init__(self, sender):
        self.messageType = "Done_M"
        self.sender = sender
        
        
    def serialize(self):
        return super().serialize()
    