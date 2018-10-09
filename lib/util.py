# -*- coding: utf-8 -*-
import time
import datetime
import json
import hashlib
import uuid
import random
import pickle
import zlib
import struct
import binascii
from copy import deepcopy
from traceback import format_exc
from contextlib import contextmanager

from tornado.log import gen_log
from tornado.gen import coroutine, sleep
from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPError
from tornado.ioloop import IOLoop


class Utils:
    """常用接口
    """
    _HTTP_ERROR_AUTO_RETRY = 5
    _HTTP_CONNECT_TIMEOUT = 5.0
    _HTTP_REQUEST_TIMEOUT = 10.0
    __FALSE_VALS = {'null', 'none', 'nil', 'false', '0', '', False, 0}

    format_exc = staticmethod(format_exc)
    log = staticmethod(gen_log)
    info = staticmethod(gen_log.info)
    debug = staticmethod(gen_log.debug)
    warning = staticmethod(gen_log.warning)
    error = staticmethod(gen_log.error)

    sleep = staticmethod(sleep)
    deepcopy = staticmethod(deepcopy)

    @classmethod
    def timestamp(cls):
        return int(time.time())

    @classmethod
    def utimestamp(cls):
        return cls.utctimestamp()

    @classmethod
    def datetime(cls, *args, **kwargs):
        if not args and not kwargs:
            return datetime.datetime.utcnow()
        return datetime.datetime(*args, **kwargs)

    @classmethod
    def today_start(cls):
        return cls.utctimestamp(3)

    @classmethod
    def stamp2time(cls, stamp=None, format_type='%Y-%m-%d %H:%M:%S'):
        if stamp is None:
            stamp = cls.timestamp()
        return time.strftime(format_type, time.localtime(stamp))

    @classmethod
    def time2stamp(cls, timestr, format_type='%Y-%m-%d %H:%M:%S'):
        return time.mktime(time.strptime(timestr, format_type))

    @classmethod
    def json_encode(cls, data):
        value = None
        try:
            value = json.dumps(data)
        except:
            pass
        return value

    @classmethod
    def json_decode(cls, data):
        value = None
        try:
            value = json.loads(data)
        except:
            pass
        return value

    @classmethod
    def uuid1(cls, node=None, clock_seq=None):
        return uuid.uuid1(node, clock_seq).hex

    @classmethod
    def md5(cls, val):
        val = val.encode('utf-8')
        return hashlib.md5(val).hexdigest()

    @classmethod
    def md5_u32(cls, val):
        val = val.encode('utf-8')
        return int(hashlib.md5(val).hexdigest(), 16) >> 96

    @classmethod
    def md5_u64(cls, val):
        val = val.encode('utf-8')
        return int(hashlib.md5(val).hexdigest(), 16) >> 64

    @classmethod
    def randint(cls, start, end):
        return random.randint(start, end)

    @classmethod
    def random(cls):
        return random.random()

    @classmethod
    @contextmanager
    def catch_error(cls, quiet=False):
        try:
            yield None
        except Exception as err:
            if quiet is False:
                cls.debug(cls.format_exc())

    @classmethod
    def pickle_dumps(cls, val):
        stream = pickle.dumps(val)
        result = zlib.compress(stream)
        return result

    @classmethod
    def pickle_loads(cls, val):
        stream = zlib.decompress(val)
        result = pickle.loads(stream)
        return result

    @staticmethod
    def _add_timeout(delay, _callable, *args, **kwargs):
        return IOLoop.current().call_later(delay, _callable, *args, **kwargs)

    @classmethod
    @coroutine
    def _fetch_url(cls, url, params=None, method='GET', *, headers=None, body=None, **kwargs):

        if params:
            url = cls.url_concat(url, params)
        if headers is None:
            headers = {}
        if isinstance(body, dict):
            content_type = headers.get('Content-Type', None)
            if content_type is None:
                headers['Content-Type'] = 'application/x-www-form-urlencoded'
                body = cls.urlparse.urlencode(body)
            elif content_type == 'application/json':
                body = cls.json_encode(body)

        result = None
        for _ in range(0, cls._HTTP_ERROR_AUTO_RETRY):

            try:
                client = AsyncHTTPClient()
                if 'connect_timeout' not in kwargs:
                    kwargs['connect_timeout'] = cls._HTTP_CONNECT_TIMEOUT
                if 'request_timeout' not in kwargs:
                    kwargs['request_timeout'] = cls._HTTP_REQUEST_TIMEOUT

                result = yield client.fetch(HTTPRequest(url, method.upper(), headers, body, **kwargs))
            except HTTPError as err:
                result = err.response
                if err.code < 500:
                    break
                else:
                    yield cls.sleep(1)
            except Exception as err:
                yield cls.sleep(1)
            else:
                break
        return result

    @classmethod
    @coroutine
    def fetch_url(cls, url, params=None, method='GET', *, headers=None, body=None, style='JSON', **kwargs):
        result = None

        response = yield cls._fetch_url(url, params, method, headers=headers, body=body, **kwargs)
        if response and response.body:
            try:
                if isinstance(style, str):
                    style = style.upper()
                if style == 'JSON':
                    result = cls.json_decode(response.body)
                elif style == 'XML':
                    result = cls.xml_decode(response.body)
                elif style == 'TEXT':
                    result = cls.basestring(response.body)
                else:
                    result = response.body

            except Exception as err:
                cls.info('{0} => {1}'.format(err, response.body))
            else:
                if style in ('JSON', 'XML', 'TEXT'):
                    # cls.debug(response.body)
                    pass
        return result

    @classmethod
    def reverseHex(cls, data):
        """
        Flip byte order in the given data (hex string).
        """
        b = bytearray(binascii.unhexlify(data))
        b.reverse()
        return binascii.hexlify(b)

    @classmethod
    def get_reward(cls, blockchain, height):
        if blockchain in ('bitcoin', 'bitcoincash', 'superbitcoin',):
            return (5000000000 >> (max(height, 0) // 210000)) * .00000001
        if blockchain in ('litecoin', 'litecoincash'):
            return (5000000000 >> (max(height, 0) // 840000)) * .00000001
        if blockchain in ('bitcoindiamond', ):
            return (50000000000 >> (max(height, 0) // 210000)) * .00000001
        return 0

    @classmethod
    def bits_to_target(cls, nbits):
        bits = struct.pack('<L', int(nbits, 16))
        return struct.unpack('<L', bits[:3] + b'\0')[0] * 2 ** (8 * (bits[3] - 3))

    @classmethod
    def diff_to_target(cls, algo, difficulty):
        if algo == 'scrypt':
            diff1 = 0x0000ffff00000000000000000000000000000000000000000000000000000000
        elif algo == 'x13sm3':
            diff1 = 0x00000000ffff0000000000000000000000000000000000000000000000000000
        else:
            diff1 = 0x00000000ffff0000000000000000000000000000000000000000000000000000
        return float(diff1) / difficulty

    @classmethod
    def calculate_pps_value(cls, mining_algo, job_target, shares, reward):
        target = cls.diff_to_target(mining_algo, shares)
        pps_value = max(float(job_target + 1) / float(target + 1) * reward, 0.0)
        return pps_value

    @classmethod
    def extract_height(cls, coinbase1):
        """

        :param coinbase1: str in notify data, params:[job_id, pre_hash, coinbase1,...]
        :return:
        """
        try:
            coinbase_placeholder = '00000000000000000000000000000000000000ffffffff'
            _, coinb2 = coinbase1.split(coinbase_placeholder)
            count = int(coinb2[2:4], 16)
            height_hex = coinb2[4: 4 + 2 * count]
            return int(cls.reverseHex(height_hex), 16)
        except:
            return 0

