"""Message Broker"""
from typing import Dict, List, Any, Tuple
import socket
import enum

class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2


class Broker:
    """Implementation of a PubSub Message Broker."""
    
    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self._host = "localhost"
        self._port = 5000
        self.topics={}
        self.subs={}
        LOGGER.info("Listen @ %s:%s", self._host, self._port)
        self.sel=selectors.DefaultSelector()
        self.sock = socket.socket()     
        self.sock.bind(('localhost', _port))
        self.sock.listen(100)
        self.sel.register(self.sock, selectors.EVENT_READ, self.accept) #the socket is ready to read



    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics."""
        return self.topics.keys()

    def get_topic(self, topic):
        if topic in self.topics:
        """Returns the currently stored value in topic."""
            return self.topics[topic]
        else:
            return null

    def put_topic(self, topic, value):
        """Store in topic the value."""
        
        self.topics[topic]=value        

    def list_subscriptions(self, topic: str) -> List[socket.socket]:
        """Provide list of subscribers to a given topic."""
        res=[]
        for elem in self.subs[topic]:
            res.append(elem[0])
        return res

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        if topic in self.subs:
            self.subs[topic].append((address,_format))
        else:
            self.subs[topic]=[(address,_format)]
            
    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        for elem in self.subs[topic]:
            if address in elem: 
                self.subs[topic].remove(elem)


    def run(self):
        """Run until canceled."""
        
        while not self.canceled:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
        pass
        

    def accept(self,sock, mask):
        conn, addr = self.sock.accept()  # Should be ready
        print('accepted', conn, 'from', addr)
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, self.read)
