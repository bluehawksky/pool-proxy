# coding=utf-8

import time
from traceback import format_exc
from tornado.log import app_log
from tornado.gen import coroutine
from kiel.clients import Producer
from lib.util import Utils
from config import MINING_ALGO, BLOCKCHAIN_NAME


class KafkaUtil:

    def __init__(self, servers, blockchian, mining_algo):
        self.servers = servers
        self.blockchain = blockchian
        self.mining_algo = mining_algo
        self.topic = '{}.share'.format(mining_algo)
        self.producer = self.initialize_kafka()

    def initialize_kafka(self):
        producer = Producer(brokers=self.servers, key_maker=self.key_maker, partitioner=self.partitioner)
        return producer

    @coroutine
    def submit_share(self, pool_type, worker_name, shares, height, nbits, ip, is_valid):
        try:
            reward = Utils.get_reward(BLOCKCHAIN_NAME, height)
            target = Utils.bits_to_target(nbits)
            pps_value = Utils.calculate_pps_value(MINING_ALGO, target, shares, reward)
            share_data = {
                'pool_type': pool_type,
                'blockchain': self.blockchain,
                'worker_name': worker_name,
                'shares': shares,
                'is_valid': is_valid,
                'timestamp': int(time.time()),
                'ip': ip,
                'height': height,
                'pps_value': pps_value,
                'bits': nbits,
            }
            app_log.info('Submit shares to kafka:{}'.format(share_data))
            yield self.producer.produce(self.topic, share_data)
        except:
            app_log.info(format_exc())
        return True

    @classmethod
    def murmur2(cls, data):
        data = bytes(data, 'utf-8')

        length = len(data)
        seed = 0x9747b28c

        m = 0x5bd1e995
        r = 24

        h = seed ^ length
        length4 = length // 4

        for i in range(length4):
            i4 = i * 4
            k = ((data[i4 + 0] & 0xff) +
                 ((data[i4 + 1] & 0xff) << 8) +
                 ((data[i4 + 2] & 0xff) << 16) +
                 ((data[i4 + 3] & 0xff) << 24))
            k &= 0xffffffff
            k *= m
            k &= 0xffffffff
            k ^= (k % 0x100000000) >> r
            k &= 0xffffffff
            k *= m
            k &= 0xffffffff

            h *= m
            h &= 0xffffffff
            h ^= k
            h &= 0xffffffff

        extra_bytes = length % 4
        if extra_bytes >= 3:
            h ^= (data[(length & ~3) + 2] & 0xff) << 16
            h &= 0xffffffff
        if extra_bytes >= 2:
            h ^= (data[(length & ~3) + 1] & 0xff) << 8
            h &= 0xffffffff
        if extra_bytes >= 1:
            h ^= (data[length & ~3] & 0xff)
            h &= 0xffffffff
            h *= m
            h &= 0xffffffff

        h ^= (h % 0x100000000) >> 13
        h &= 0xffffffff
        h *= m
        h &= 0xffffffff
        h ^= (h % 0x100000000) >> 15
        h &= 0xffffffff

        return h

    @classmethod
    def key_maker(cls, msg):
        return msg["worker_name"].split('.')[0]

    @classmethod
    def partitioner(cls, key, partitions):
        idx = (cls.murmur2(key) & 0x7fffffff) % len(partitions)
        return idx
