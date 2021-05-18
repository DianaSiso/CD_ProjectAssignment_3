"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
from .broker import Broker
import socket
from .protocol import CDProto



class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.queue= Queue.LifoQueue()
        self.topic=topic
        self._type=_type
        self.sel=selectors.DefaultSelector()
        self.sock = socket.socket()
        self.sock.connect_ex(('localhost', 5000))
        self.sel.register(self.sock, selectors.EVENT_READ,self.rcvmsg) #ao receber algo vai ler
        
        


    def push(self, value):
        """Sends data to broker. """
        if(self.queue_type==json):
            mespush=protocol.push(self.topic,value).__str__json()
        if(self.queue_type==pickle):
            mespush=protocol.push(self.topic,value).__str__pickle()
        CDProto.send_msg(self.sock,mespush)
        #self.broker.put_topic(self.topic,value)



    def pull(self) -> (str, str):
        """Receives (topic, data) from broker.

        Should BLOCK the consumer!"""
        if(self.queue_type==json):
            mespull=protocol.pull(self.topic,value).__str__json()
        if(self.queue_type==pickle):
            mespull=protocol.pull(self.topic,value).__str__pickle()
        CDProto.send_msg(self.sock,mespull)
        value=CDProto.recv_msg(self.sock,self.queue_type).__str__() #bloqueia
        while(value==None):
            value=CDProto.recv_msg(self.sock,self.queue_type)__str__() #bloqueia
        return (self.topic,value)



    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        #self.broker.list_topics()
        if(self.queue_type==json):
            mesl=protocol.lists().__str__json()
        if(self.queue_type==pickle):
            mesl=protocol.lists().__str__pickle()
        CDProto.send_msg(self.sock,mesl)


    def cancel(self):
        """Cancel subscription."""
        if(self.queue_type==json):
            mesccl=protocol.cancel(self.topic).__str__json()
        if(self.queue_type==pickle):
            mesccl=protocol.cancel(self.topic).__str__pickle()
        CDProto.send_msg(self.sock,mesccl)
        #self.broker.unsubscribe(self.topic,self.queue) #precisamos de um endereço e serializer


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    self.queue_type=json
    if(Queue._type=MiddlewareType.CONSUMER) :
        mesreg=protocol.register(self.topic).__str__json()
        CDProto.send_msg(self.sock,mesreg)
    



class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    self.queue_type=xml
    if(Queue._type=MiddlewareType.CONSUMER) :
            

class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    self.queue_type=pickle
    if(Queue._type==MiddlewareType.CONSUMER) :
        mesreg=protocol.register(self.topic).__str__pickle()
        CDProto.send_msg(self.sock,mesreg)
           
    