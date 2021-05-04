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
        


    def push(self, value):
        """Sends data to broker. """
        self.broker.put_topic(self.topic,value)


    def pull(self) -> (str, tuple):
        data= self.broker.get_topic(self.topic)
        #de onde vem o tuple


    def pull(self) -> (str, tuple):
        """Receives (topic, data) from broker.

        Should BLOCK the consumer!"""


    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        self.broker.list_topics()


    def cancel(self):
        """Cancel subscription."""
        self.broker.unsubscribe(self.topic) #precisamos de um endereço e serializer


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    self.broker.subscribe(self.topic)#precisamos de um endereço e serializer



class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""


class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""

