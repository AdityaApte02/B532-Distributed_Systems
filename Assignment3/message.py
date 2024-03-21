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
        
        
class PulseMessageMapper(Message):
    def __init__(self, sender, messageType="PULSE_M"):
        super().__init__(messageType, sender)
        
    def serialize(self):
        return super().serialize()
    
    
class PulseMessageReducer(Message):
    def __init__(self, sender, messageType = "PULSE_R"):
        super().__init__(messageType, sender)
        
    def serialize(self):
        return super().serialize()
    
    
class DoneMessageMapper(Message):
    def __init__(self, sender, messageType = "DONE_M"):
        super().__init__(messageType, sender)
        
    def serialize(self):
        return super().serialize()
    
    
class SendToMapperMessage(Message):
    def __init__(self, reducer, messageType="SEND_MAPPER", sender="MASTER"):
        super().__init__(messageType, sender)
        self.reducer = reducer
        
    def serialize(self):
        s = ''
        for k,v in self.reducer.items():
            s = s + " " +str(v)
        return super().serialize() + s
    
    
class sendToReducerMessage(Message):
    def __init__(self, id, key, value, messageType="SEND_REDUCER"):
       super().__init__(messageType, id)
       self.key = key
       self.value = value
        
    def serialize(self):
        s = self.key + str(self.value)
        return super().serialize() + " " + s
    