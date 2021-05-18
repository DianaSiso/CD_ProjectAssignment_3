"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
from .broker import Broker
import socket
from .protocol import CDProto
from typing import Any


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
        if(self.queue_type==1):
            mespush=protocol.push(self.topic,value).__str__json()
        if(self.queue_type==2):
            mespush=protocol.push(self.topic,value).__str__pickle()
        CDProto.send_msg(self.sock,mespush,self.queue_type)
        #self.broker.put_topic(self.topic,value)



    def pull(self) -> (str, Any):
        """Waits for (topic, data) from broker.

        Should BLOCK the consumer!"""
        if(self.queue_type==1):
            mespull=protocol.pull(self.topic,value).__str__json()
        if(self.queue_type==2):
            mespull=protocol.pull(self.topic,value).__str__pickle()
        CDProto.send_msg(self.sock,mespull,self.queue_type)
        value=CDProto.recv_msg(self.sock).__str__() #bloqueia
        while(value==None):
            value=CDProto.recv_msg(self.sock)__str__() #bloqueia
        return (self.topic,value)



    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        #self.broker.list_topics()
        if(self.queue_type==1):
            mesl=protocol.lists().__str__json()
        if(self.queue_type==2):
            mesl=protocol.lists().__str__pickle()
        CDProto.send_msg(self.sock,mesl,self.queue_type)


    def cancel(self):
        """Cancel subscription."""
        if(self.queue_type==1):
            mesccl=protocol.cancel(self.topic).__str__json()
        if(self.queue_type==2):
            mesccl=protocol.cancel(self.topic).__str__pickle()
        CDProto.send_msg(self.sock,mesccl,self.queue_type)
        #self.broker.unsubscribe(self.topic,self.queue) #precisamos de um endereço e serializer


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    self.queue_type=1
    if(Queue._type==MiddlewareType.CONSUMER) :
        mesreg=protocol.register(self.topic).__str__json()
        CDProto.send_msg(self.sock,mesreg,1)
    



class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    self.queue_type=0
    if(Queue._type==MiddlewareType.CONSUMER) :
            

class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    self.queue_type=2
    if(Queue._type==MiddlewareType.CONSUMER) :
        mesreg=protocol.register(self.topic).__str__pickle()
        CDProto.send_msg(self.sock,mesreg,2)
           
    