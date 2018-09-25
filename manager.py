# coding=utf-8

import os
import argparse
import signal
from traceback import format_exc
import tornado
from tornado.options import options
from tornado.log import gen_log
from tornado.platform.asyncio import AsyncIOMainLoop
from proxy.proxy import ProxyTCPServer
from proxy import init_kafka, stop_kafka
from config import LOG_CONFIG, HOST, PORT, MULTI_PROCESS_ENABLE


def parse_args():
    parser = argparse.ArgumentParser(description='Mining Pool Proxy.')
    parser.add_argument('--logfile', dest='log_file', type=str, default='')
    parser.add_argument('--port', dest='port', type=int, default=0)
    parser.add_argument('--process', dest='process', type=int, default=1)
    args = parser.parse_args()
    return args


def sig_handler(signum, frame):
    if signum in (signal.SIGINT, signal.SIGTERM,):
        gen_log.info('Stop Pool proxy!')
        tornado.ioloop.IOLoop.current().stop()


class ServerEntry:
    def __init__(self):
        self.init_options()
        # AsyncIOMainLoop().install()
        tornado.ioloop.IOLoop.instance().run_sync(init_kafka)

    def init_options(self):
        options.logging = LOG_CONFIG['level']
        log_file = args.log_file or LOG_CONFIG['filename']

        if log_file:
            log_dir = os.path.dirname(log_file)
            log_basename = os.path.basename(log_file)
            if os.path.exists(log_dir) is False:
                os.makedirs(log_dir)

            options.log_rotate_mode = 'time'
            options.log_rotate_when = 'midnight'
            options.log_file_num_backups = LOG_CONFIG['backups']
            options.log_file_prefix = '{}/{}'.format(log_dir, log_basename)
        options.parse_command_line(args=[])

    def run(self):
        try:
            gen_log.info('Starting Pool proxy...')

            server = ProxyTCPServer()

            if MULTI_PROCESS_ENABLE is True:
                process_count = args.process or tornado.process.cpu_count()
                sockets = tornado.netutil.bind_sockets(args.port or PORT, address=HOST)
                tornado.process.fork_processes(process_count)
                server.add_sockets(sockets)
            else:
                server.listen(args.port or PORT, address=HOST)

            tornado.ioloop.IOLoop.current().start()
        except KeyboardInterrupt:
            gen_log.info('Stop Pool proxy by CTRL + C')
            tornado.ioloop.IOLoop.instance().run_sync(stop_kafka)
            tornado.ioloop.IOLoop.current().stop()
        except:
            gen_log.error(format_exc())


if __name__ == "__main__":
    # signal.signal(signal.SIGTERM, sig_handler)
    # signal.signal(signal.SIGINT, sig_handler)
    args = parse_args()
    ServerEntry().run()


