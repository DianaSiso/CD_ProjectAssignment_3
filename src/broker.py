"""Message Broker"""
from typing import Dict, List, Any, Tuple
import socket
import enum
from .protocol import CDProto
import selectors

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
        self.sel=selectors.DefaultSelector()
        self.sock = socket.socket()     
        self.sock.bind(('localhost', self._port))
        self.sock.listen(100)
        self.sel.register(self.sock, selectors.EVENT_READ, self.accept) #the socket is ready to read



    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics."""
        newlist= list()
        for i in self.topics.keys():
            newlist.append(i)
        return newlist

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        if topic in self.topics:
            return self.topics[topic]
        else:
            return None

    def put_topic(self, topic, value):
        """Store in topic the value."""
        self.topics[topic]=value        

    def list_subscriptions(self, topic: str) -> List[socket.socket]:
        """Provide list of subscribers to a given topic."""
        return self.subs[topic]

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
        #print('accepted', conn, 'from', addr)
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, self.read)

    
    def read(self,conn, mask):
            data,ser = CDProto.recv_msg(conn)  #the server reads the message sent through the socket
            
            comm=data['command']

            if comm=="register":
                self.subscribe(data['topic'], conn,ser)
                if(ser==1):
                    msg= CDProto.reppull(self.get_topic(data['topic'])).__str__json()
                elif(ser==2):
                    msg= CDProto.reppull(self.get_topic(data['topic'])).__str__pickle()
                elif(ser==0):
                    msg= CDProto.reppull(self.get_topic(data['topic'])).__str__xml()
                CDProto.send_msg(conn, msg, int(ser))
            elif comm=="cancel":
                self.unsubscribe(data['topic'], conn)
            elif comm=="lists":
                self.list_topics()
            elif comm=="push":  #é sempre um produtor que vai usar este comando
                res = self.list_subscriptions(data['topic'])
                self.put_topic(data['topic'], data['value'])
                for element in res:
                    if element[1] == 1:
                        msg = CDProto.reppull(data['value']).__str__json()
                    elif element[1] == 2:
                        msg = CDProto.reppull(data['value']).__str__pickle()
                    elif element[1] == 0:
                        msg = CDProto.reppull(data['value']).__str__xml()
                    CDProto.send_msg(element[0], msg, element[1])

            #elif comm=="pull":
            #    msg= CDProto.reppull(self.get_topic(data['topic']))
            #    CDProto.send_msg(conn, msg, data['serializer'])


          
    
        #else:
        #    print('closing', conn)
        #    logging.debug('---> client unregistered')
        #    for key in self.clients:
        #        if conn in self.clients[key]:
        #            self.clients[key].remove(conn)
        #    logging.debug(self.clients)
        #    self.sel.unregister(conn)
        #    conn.close()