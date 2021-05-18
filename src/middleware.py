"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
from .broker import Broker
import socket


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
        msg = {'tipo': "push", 'data':value}
        sendmsg(self.sock,msg)
        
        #self.broker.put_topic(self.topic,value)



    def pull(self) -> (str, tuple):
        """Receives (topic, data) from broker.

        Should BLOCK the consumer!"""
        sendmsg("dame o value do topic")
        rec("é este o value")
        data= rcvmsg

        data=self.broker.get_topic(self.topic)
        while data==null:
            data= self.broker.get_topic(self.topic)
        return (self.topic,data)
        #de onde vem o tuple


        


    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        self.broker.list_topics()


    def cancel(self):
        """Cancel subscription."""
        self.broker.unsubscribe(self.topic,self.queue) #precisamos de um endereço e serializer


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    if(Queue._type=MiddlewareType.CONSUMER) :
            self.broker.subscribe(Queue.topic, Queue.sock, 0)
    



class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    if(Queue._type=MiddlewareType.CONSUMER) :
            self.broker.subscribe(Queue.topic, Queue.sock, 1)

class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    if(Queue._type=MiddlewareType.CONSUMER) :
            self.broker.subscribe(Queue.topic, Queue.sock, 2)

    def sendmsg(cls,connection: socket ,msg:Dict):      
        msgBytes=pickle.dumps(msg).encode('utf-8')
        lenBytes=len(msgBytes).to_bytes(2,'big')
        connection.sendall(lenBytes+msgBytes)
    def rcvmsg(cls,connection:socket):

        header=connection.recv(2) #recevemos os 2 primeiros bits
        head=int.from_bytes(header,byteorder='big') #contem o tamanho da mensagem 
        if head!=0:
            message=connection.recv(head) #recebemos os bits correspondente á mensagem
            datat=message.decode(encoding='UTF-8').replace("None",'"null"') #descodificamos a mensagem 
            try:
                data=json.loads(datat) # vira json
                if data.get('command') == "join": 
                    return cls.join(data.get('channel'))
                if data.get('command') == "message":
                    return cls.message(data.get('message'),data.get('channel'))
                if data.get('command') == "register":
                    return cls.register(data.get('user'))
            except:
                raise CDProtoBadFormat
        else:
            return None