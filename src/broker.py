"""Message Broker"""
from typing import Dict, List, Any, Tuple
import socket
from serializer import serializable

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



    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics."""
        return self.topics.keys()

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        return self.topics[topic]

    def put_topic(self, topic, value):
        """Store in topic the value."""
        self.topics[topic]=[value]
        

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
            pass
