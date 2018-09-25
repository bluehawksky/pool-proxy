# -*- encoding:utf-8 -*-

import time
from twisted.internet import reactor
from twisted.internet.protocol import Protocol, ClientCreator, ClientFactory

# import settings


class ClientProtocol(Protocol):
    def connectionMade(self):
        self.uuid = time.time()
        print("connectionMade uuid:{}".format(self.uuid))
        # print(dir(self))
        # print(dir(self.transport))
        # for attr in dir(self.transport):
        #     print((attr, getattr(self.transport, attr)))
        #
        # print(dir(self.transport.connector))
        # print(dir(self.transport.protocol))
        self.transport.write(b'> To Server:Hello server')

    def dataReceived(self, data):
        print("connectionMade uuid:{}".format(self.uuid))
        print('> From Server:%s' % data)
        # reactor.stop()
        # self.transport.write(data)


class ProxyClientFactory(ClientFactory):
    protocol = ClientProtocol

    def startedConnecting(self, connector):
        print('Started to connect.')

    def buildProtocol(self, addr):
        print('Connected.')
        # print(dir(self))

        return ClientProtocol()

    def clientConnectionLost(self, connector, reason):
        print('Lost connection. Reason:', reason)
        # connector.connect()


    def clientConnectionFailed(self, connector, reason):
        print('Connection failed. Reason:', reason)


class StratumProxy(object):

    _proxy_connections = {}

    @classmethod
    def add_proxy_connection(cls, conn):
        pass

    @classmethod
    def remove_proxy_connection(cls, conn):
        pass

    @classmethod
    def gotProtocol(cls, p):
        p.sendMessage("Hello")
        # TODO

        print ('call gotProtocol')
        print(dir(p))
        # reactor.callLater(1, p.sendMessage, "This is sent in a second")
        # reactor.callLater(2, p.transport.loseConnection)

    @classmethod
    def connect_to_proxy_pool(cls):
        reactor.connectTCP('127.0.0.1', 8889, ProxyClientFactory())


if __name__ == "__main__":
    StratumProxy.connect_to_proxy_pool()
    reactor.run()
