# coding=utf-8

import time
import uuid
from traceback import format_exc
from tornado.iostream import StreamClosedError
from tornado.log import app_log
from tornado.tcpclient import TCPClient
from tornado.gen import coroutine
from tornado.escape import json_decode, json_encode

from proxy.auth import miner_auth
from proxy.error import ErrorCode, ErrorMsg
from lib.util import Utils
from config import POOL_MINER_NAME, JOB_CLEAR_TIME, SUBMIT_JOB_CLEAR_TIME, DEFAULT_FORWARD_URL, DEFAULT_POOL_TYPE


class PoolConnection(object):

    __slots__ = ['worker_connections', 'pool_connections']

    def __init__(self):
        self.worker_connections = {}
        self.pool_connections = {}
        app_log.info('init pool connection')


class WorkerConnection(object):
    """
    WorkerConnection is the base Class for server and client communication.
    It will keep a dict to save all connections of workers
    """
    __slots__ = ['_uuid', '_address', '_stream', 'pool_type', 'pool_host', 'pool_port',
                 'miner_name', 'miner_name_real', 'first_job_msg',
                 'first_difficult_msg', 'subscribe_msg', 'subscribe_id',
                 'authorize_id', 'authorize_status', 'extraNonce1_real', 'extraNonce1', 'extraNonce2_size',
                 'difficulty', 'jobs', 'submit_jobs', 'is_keepalive']

    conns = PoolConnection()

    def __init__(self, stream, address):
        self._uuid = uuid.uuid1().hex
        self._address = address
        self._stream = stream
        self._stream.set_close_callback(self.on_close)

        self.pool_type = -1
        self.pool_host = ''
        self.pool_port = 0
        self.miner_name = ''
        self.miner_name_real = ''

        self.first_difficult_msg = ''
        self.first_job_msg = ''
        self.subscribe_msg = ''
        self.subscribe_id = 0
        self.authorize_id = 0
        self.authorize_status = False

        self.extraNonce1_real = ''
        self.extraNonce1 = '00'
        self.extraNonce2_size = 7
        self.difficulty = 1
        self.jobs = {}
        self.submit_jobs = {}
        self.is_keepalive = False
        WorkerConnection.conns.worker_connections[self._uuid] = self
        app_log.info("New worker connection:{} ".format(address))

    @coroutine
    def read_message(self):
        try:
            yield self._stream.read_until(b'\n', self.handle_message)
        except StreamClosedError:
            pass

    @coroutine
    def handle_message(self, msg):
        try:
            # for keepalive message
            if b'PROXY TCP4' in msg:
                self.is_keepalive = True
                yield self.read_message()
                return

            app_log.info('Worker [{}@{}] received data: {}'.format(self._uuid, self.miner_name_real, msg))

            if self.pool_type >= 0:
                data = json_decode(msg)
                method = data.get('method', '')
                if method == 'mining.submit':
                    yield self.handle_submit(data)
                else:
                    pool_conn = self.get_pool_conn()
                    if pool_conn:
                        yield pool_conn.send_message(msg)
            else:
                # Not ensure pool type yet
                data = json_decode(msg)
                method = data.get('method', '')

                if method == 'mining.subscribe':
                    self.subscribe_msg = msg
                    self.subscribe_id = data['id']
                    yield self.reply_subscribe()
                elif method == 'mining.authorize':
                    yield self.handle_authorize(data)
        except:
            app_log.info(format_exc())

        yield self.read_message()

    @coroutine
    def connect_pool(self, conn_uuid):
        """

        :param forward_type: str my: no forward, thirdpary: forward
        :param conn_uuid:
        :return:
        """
        try:
            client = ProxyTCPClient(self.pool_host, self.pool_port, self.pool_type, conn_uuid)
            yield client.connect_server(self.subscribe_msg)
            return client
        except:
            app_log.info(format_exc())
            app_log.info('Connect to pool {}:{} failed ! pool type:{}'
                         ''.format(self.pool_host, self.pool_port, self.pool_type))
        return None

    @coroutine
    def handle_authorize(self, data):
        self.authorize_id = data['id']
        self.miner_name_real = data['params'][0]

        _miner_name = self.miner_name_real.split('.')
        if len(_miner_name) == 1:
            # yield self.send_result(data['id'], ErrorCode.INVALID_MINER_NAME)
            # return self._stream.close()
            worker_name = 'default'
        else:
            worker_name = _miner_name[1]

        auth_ret = yield miner_auth(_miner_name[0])
        app_log.debug('Auth data:{}'.format(auth_ret))
        # {"sha256": "bitcoin","forward_url": "", "forward_name":"", "pool_type":0}
        if not auth_ret:
            self.pool_type = DEFAULT_POOL_TYPE
            forward_url = DEFAULT_FORWARD_URL
            forward_miner_name = POOL_MINER_NAME
        else:
            self.pool_type = auth_ret.get('pool_type', 0)
            forward_miner_name = auth_ret.get('forward_miner_name', POOL_MINER_NAME)
            forward_url = auth_ret.get('forward_url', '')
            if not forward_url:
                self.pool_type = DEFAULT_POOL_TYPE
                forward_url = DEFAULT_FORWARD_URL
            if not forward_miner_name:
                forward_miner_name = POOL_MINER_NAME

        share_forward = forward_url.split(':')
        if len(share_forward) != 2:
            yield self.send_result(data['id'], ErrorCode.INVALID_POOL_ADDRESS)
            return

        self.pool_host, self.pool_port = share_forward[0], int(share_forward[1])
        app_log.debug('Pool Host:{}, Port:{}'.format(self.pool_host, self.pool_port))
        pool_conn = yield self.connect_pool(self._uuid)
        if not pool_conn:
            yield self.send_result(data['id'], ErrorCode.CONNECT_LOST)
            return self._stream.close()

        self.append_pool_conn(pool_conn)

        # replace miner name
        if self.pool_type == 0:
            self.miner_name = self.miner_name_real
        else:
            self.miner_name = '.'.join([forward_miner_name, worker_name])
            data['params'][0] = self.miner_name
        yield pool_conn.send_message(json_encode(data))

    @coroutine
    def handle_submit(self, data):
        data['params'][2] = self.extraNonce1 + data['params'][2]

        pool_conn = self.get_pool_conn()
        if not pool_conn:
            yield self.send_result(data['id'], ErrorCode.CONNECT_LOST)
            return self._stream.close()

        if self.pool_type > 0:
            submit_id = data['id']
            data['params'][0] = self.miner_name
            job_id = data['params'][1]
            self.submit_jobs[submit_id] = {'job_id': job_id, 'timestamp': int(time.time())}

            self.clear_jobs()
        # app_log.debug('Send submit data to pool:{}'.format(data))
        yield pool_conn.send_message(json_encode(data))

    @coroutine
    def handle_set_difficulty(self, msg, difficulty):
        self.difficulty = difficulty
        if self.authorize_status is True:
            yield self.send_message(msg)
        else:
            self.first_difficult_msg = msg
            app_log.debug('handle_set_difficulty:Not authorized yet for worker:{}'.format(self._address))

    @coroutine
    def handle_notify(self, data, job_id, clear_job):
        data['params'][2] += self.extraNonce1_real

        if self.pool_type > 0:
            self.clear_jobs(clear_job)
            self.jobs[job_id] = {
                'difficulty': self.difficulty,
                'nbits': data['params'][-3],
                'height': Utils.extract_height(data['params'][2]),
                'timestamp': int(time.time())
            }
        if self.authorize_status is False:
            self.first_job_msg = json_encode(data)
            app_log.debug('handle_notify:Not authorized yet for worker:{}'.format(self._address))
            return
        yield self.send_message(json_encode(data))

    @coroutine
    def handle_stratum_response(self, msg):
        data = json_decode(msg)
        resp_id = data.get('id', None)
        if resp_id is None:
            yield self.send_message(msg)
            return

        if resp_id == self.subscribe_id:
            if data['error'] is not None:
                self._stream.close()
                return
            self.extraNonce1_real = data['result'][1]
            app_log.debug(('extraNonce1_real:{}'.format(self.extraNonce1_real)))
            return
        elif resp_id == self.authorize_id:
            if data['error'] is not None:
                yield self.send_message(msg)
                self._stream.close()
                return

            self.authorize_status = True
            yield self.send_message(msg)

            if self.first_difficult_msg:
                yield self.send_message(self.first_difficult_msg)
                self.first_difficult_msg = ''
            if self.first_job_msg:
                yield self.send_message(self.first_job_msg)
                self.first_job_msg = ''
            return
        elif resp_id in self.submit_jobs.keys():
            if self.pool_type > 0:
                job_id = self.submit_jobs.pop(resp_id, {}).get('job_id', '')
                job = self.jobs.get(job_id, None)
                if job:
                    kafka_data = {
                        'pool_type': self.pool_type,
                        'worker_name': self.miner_name_real,
                        'shares': job['difficulty'],
                        'height': job['height'],
                        'nbits': job['nbits'],
                        'ip': self._address[0],
                        'is_valid': True if data['result'] else False
                    }
                    yield WorkerConnection.kafka_producer.submit_share(**kafka_data)
        yield self.send_message(msg)

    @coroutine
    def send_result(self, resp_id, error_code=None):
        # {"id":1,"result":null,"error":[29,"Invalid username",null]}
        # {"id":1,"result":true,"error":null}
        if error_code is None:
            params = {
                "id": resp_id,
                "result": True,
                "error": None,
            }
        else:
            params = {
                "id": resp_id,
                "result": None,
                "error": [error_code, ErrorMsg.get(error_code, ''), None],
            }
        app_log.debug('send result to worker:{}'.format(params))
        yield self.send_message(json_encode(params))

    @coroutine
    def reply_subscribe(self, result=None):
        # {"error":null,"id":0,"result":[["mining.notify","ae6812eb4cd7735a302a8a9dd95cf71f"],"00",8]}
        subs = [["mining.notify", "ae6812eb4cd7735a302a8a9dd95cf71f"], self.extraNonce1, self.extraNonce2_size]
        params = {
            "id": self.subscribe_id,
            "result": result or subs,
            "error": None
        }
        yield self.send_message(json_encode(params))

    @coroutine
    def send_message(self, message):
        app_log.info('Send message to worker [{}@{}]: {}'.format(self._uuid, self.miner_name_real, message))
        if isinstance(message, str):
            message = bytes(message, encoding='utf-8')
        try:
            if message[-1] != b'\n':
                message += b'\r\n'
            yield self._stream.write(message)
        except StreamClosedError:
            self.on_close()
        except :
            app_log.info(format_exc())

    def clear_jobs(self, is_all=False):

        # if is_all is True:
        #     app_log.info('Clear all jobs')
        #     self.jobs = {}
        #     self.submit_jobs = {}
        #     return

        app_log.info('Clear expired jobs')
        now_time = int(time.time())
        try:
            for job_id in list(self.jobs.keys()):
                if now_time - self.jobs[job_id]['timestamp'] >= JOB_CLEAR_TIME:
                    del self.jobs[job_id]
        except:
            app_log.info(format_exc())
        try:
            for submit_id in list(self.submit_jobs.keys()):
                if now_time - self.submit_jobs[submit_id]['timestamp'] >= SUBMIT_JOB_CLEAR_TIME:
                    # app_log.debug('Clear sumbimt id:{}'.format(submit_id))
                    del self.submit_jobs[submit_id]
        except:
            app_log.info(format_exc())

    def get_pool_conn(self):
        try:
            return WorkerConnection.conns.pool_connections.get(self._uuid, None)
        except:
            app_log.info(format_exc())
            return None

    def append_pool_conn(self, conn):
        try:
            WorkerConnection.conns.pool_connections[self._uuid] = conn
        except:
            app_log.info(format_exc())

    def on_close(self):
        WorkerConnection.conns.worker_connections.pop(self._uuid, None)
        pool_conn = WorkerConnection.conns.pool_connections.pop(self._uuid, None)
        self.close_conn(pool_conn)
        if self.is_keepalive is False:
            app_log.info("Worker connection has been closed. uuid:{}. worker_name:{}"
                         "".format(self._uuid, self.miner_name_real))

    @classmethod
    def close_conn(cls, conn):
        try:
            if conn:
                app_log.info('Close conn:{}. uuid:{}'.format(conn, conn._uuid))
                conn._stream.close()
        except:
            app_log.info(format_exc())


class ProxyTCPClient(TCPClient):
    """
    ProxyTCPClient is the Base class for pool connection
    """

    def __init__(self, host, port, pool_type, conn_uuid):
        super().__init__()
        self.host = host
        self.port = port
        self.pool_type = pool_type
        self._uuid = conn_uuid
        self._stream = None

    def on_close(self):
        try:
            app_log.debug('Pool connection closed. uuid:{}'.format(self._uuid))
            WorkerConnection.conns.pool_connections.pop(self._uuid, None)
            worker_conn = WorkerConnection.conns.worker_connections.pop(self._uuid, None)
            if worker_conn:
                worker_conn._stream.close()
        except:
            app_log.debug(format_exc())

    @coroutine
    def connect_server(self, subscribe_data):
        self._stream = yield self.connect(self.host, self.port)
        yield self._stream.write(subscribe_data)
        app_log.info('Send subscribe to pool:{}'.format(subscribe_data))

        self._stream.set_close_callback(self.on_close)
        self._stream.read_until(b'\n', self.handle_message)
        return self._stream

    @coroutine
    def send_message(self, message):
        """

        :param message: bytes
        :return:
        """
        try:
            app_log.info('Send message to pool [{}]: {}'.format(self._uuid, message))
            if isinstance(message, str):
                message = bytes(message, encoding='utf-8')
            if message[-1] != b'\n':
                message += b'\r\n'
            yield self._stream.write(message)
        except StreamClosedError:
            self.on_close()
        except:
            app_log.debug(format_exc())

    @coroutine
    def read_message(self):
        try:
            yield self._stream.read_until(b'\n', self.handle_message)
        except StreamClosedError:
            self.on_close()

    @coroutine
    def handle_message(self, msg):
        try:
            app_log.debug('Pool connection [{}] recieved message: {}'.format(self._uuid, msg))

            worker_conn = self.get_worker_conn()
            if not worker_conn:
                app_log.info('Worker connection not found. uuid:{}'.format(self._uuid))
                app_log.debug(WorkerConnection.conns.worker_connections)
                WorkerConnection.conns.pool_connections.pop(self._uuid, None)
                self._stream.close()
                return

            data = json_decode(msg)
            resp_id = data.get('id', None)
            method = data.get('method', '')

            if method == 'mining.set_difficulty':
                yield worker_conn.handle_set_difficulty(msg, data['params'][0])
            elif method == 'mining.notify':
                job_id, clear_jobs = data['params'][0], data['params'][-1]
                yield worker_conn.handle_notify(data, job_id, clear_jobs)
            elif resp_id is not None:
                yield worker_conn.handle_stratum_response(msg)
            else:
                yield worker_conn.send_message(msg)
        except:
            app_log.error(format_exc())

        yield self.read_message()

    def get_worker_conn(self):
        return WorkerConnection.conns.worker_connections.get(self._uuid, None)
