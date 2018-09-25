# coding=utf-8


from tornado.tcpserver import TCPServer
from tornado.gen import coroutine
# from tornado.iostream import StreamClosedError
from proxy.connection import WorkerConnection


class ProxyTCPServer(TCPServer):

    @coroutine
    def handle_stream(self, stream, address):
        conn = WorkerConnection(stream=stream, address=address)
        yield conn.read_message()

    # @coroutine
    # def handle_stream2(self, stream, address):
    #     while True:
    #         try:
    #             data = yield stream.read_until(b"\n")
    #             yield stream.write(data)
    #         except StreamClosedError:
    #             break

