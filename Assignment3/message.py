from abc import ABC, abstractmethod
class Message(ABC):
    def __init__(self, messageType, sender):
        self.messageType = messageType
        self.sender = sender
        
    @abstractmethod
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
    

class DoneMessageReducer(Message):
    def __init__(self, sender, messageType = "DONE_R"):
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
    
    
class Terminate(Message):
    def __init__(self, sender="MASTER", messageType="TERMINATE"):
        super().__init__(messageType, sender)
        
    def serialize(self):
        return super().serialize()
    
    
class SendDoneToReducer(Message):
    def __init__(self, sender, messageType="DONE_MAPPER_REDUCER"):
        super().__init__(messageType, sender)
        
    def serialize(self):
        return super().serialize()
    
class SendToReducerMessage(Message):
    def __init__(self, id, key, values, messageType="SEND_REDUCER"):
        super().__init__(messageType, id)
        self.key = key
        if all(isinstance(x, int) for x in values):
            self.value = sum(values)
            
        else:
            self.value = values[0]
        
    def serialize(self):
        s = self.key + str(self.value)
        return super().serialize() + " " + s
    
    
class ClearMessage(Message):
    def __init__(self, messageType, sender = "MASTER"):
        super().__init__(messageType, sender)
        
        
    def serialize(self):
        return super().serialize()
    