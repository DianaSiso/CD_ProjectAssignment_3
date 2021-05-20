"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
from .broker import Broker
import socket
from .protocol import CDProto
from typing import Any
import json
import selectors

class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.canceled = False
        self.queue=LifoQueue()
        self.topic=topic
        self._type=_type
        self.sel=selectors.DefaultSelector()
        self.sock = socket.socket()
        self.sock.connect(('localhost', 5002))
        self.queue_type = 0
        self.sel.register(self.sock, selectors.EVENT_READ,self.accept) #ao receber algo vai ler
        
    def accept(self,sock, mask):
        conn, addr = self.sock.accept()  # Should be ready
        #print('accepted', conn, 'from', addr)
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, self.pull)

    def run(self):
        """Run until canceled."""
        
        while not self.canceled:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
        pass

    def push(self, value):
        """Sends data to broker. """
        mespush = ""
       
        if(self.queue_type==1):
            mespush=CDProto.push(self.topic,value).__str__json()
        if(self.queue_type==2):
            mespush=CDProto.push(self.topic,value).__str__pickle()
        if(self.queue_type==0):
            mespush=CDProto.push(self.topic,value).__str__xml()
            
        CDProto.send_msg(self.sock,mespush,self.queue_type)
        #self.broker.put_topic(self.topic,value)



    def pull(self):
        """Waits for (topic, data) from broker.

        Should BLOCK the consumer!"""
        
        things,ser = CDProto.recv_msg(self.sock)
        value=things['value']
        print('value:' + str(value))
        return self.topic, value
        

        #value=CDProto.recv_msg(self.sock).__str__() #bloqueia
        #while(value==None):
        #    value=CDProto.recv_msg(self.sock).__str__()  #bloqueia
        #return (self.topic, value)



    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        #self.broker.list_topics()
        if(self.queue_type==1):
            mesl=CDProto.lists().__str__json()
        if(self.queue_type==2):
            mesl=CDProto.lists().__str__pickle()
        if(self.queue_type==0):
            mesl=CDProto.lists().__str__xml()
        CDProto.send_msg(self.sock,mesl,self.queue_type)


    def cancel(self):
        """Cancel subscription."""
        if(self.queue_type==1):
            mesccl=CDProto.cancel(self.topic).__str__json()
        if(self.queue_type==2):
            mesccl=CDProto.cancel(self.topic).__str__pickle()
        if(self.queue_type==0):
            mesccl=CDProto.cancel(self.topic).__str__xml()
        CDProto.send_msg(self.sock,mesccl,self.queue_type)
        #self.broker.unsubscribe(self.topic,self.queue) #precisamos de um endereço e serializer


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization.""" 
    
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.queue_type = 1
        self.run()


    def run(self):
        if(self._type==MiddlewareType.CONSUMER) :
            temp = CDProto.register(self.topic)
            mesreg=temp.__str__json()
            CDProto.send_msg(self.sock,mesreg,1)
        




class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.queue_type = 0
        self.run()

    def run(self):
        if(self._type==MiddlewareType.CONSUMER) :
            temp = CDProto.register(self.topic)
            mesreg=temp.__str__xml()
            CDProto.send_msg(self.sock,mesreg,0)        

class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.queue_type = 2
        self.run()

    def run(self):
        if(self._type==MiddlewareType.CONSUMER) :
            temp = CDProto.register(self.topic)
            mesreg=temp.__str__pickle()
            CDProto.send_msg(self.sock,mesreg,2)
    