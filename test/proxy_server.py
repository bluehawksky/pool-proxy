# -*- encoding:utf-8 -*-

import sys
from twisted.internet import protocol, reactor, endpoints


class Echo(protocol.Protocol):
    def dataReceived(self, data):
        print(data)
        self.transport.write(data)

    def clientConnectionLost(self, connector, reason):
        print('Lost connection. Reason:', reason)


class EchoFactory(protocol.Factory):
    def buildProtocol(self, addr):
        print(addr)
        return Echo()


if __name__ == '__main__':
    port = sys.argv[1]
    reactor.listenTCP(int(port), EchoFactory())
    # endpoints.serverFromString(reactor, "tcp:1234").listen(EchoFactory())
    reactor.run()
