# coding=utf-8

import time
from traceback import format_exc
from tornado.log import app_log
from tornado.gen import coroutine
from kiel.clients import Producer
from lib.util import Utils
from config import MINING_ALGO


class KafkaUtil:

    def __init__(self, servers, blockchian, mining_algo):
        self.servers = servers
        self.blockchain = blockchian
        self.mining_algo = mining_algo
        self.topic = '{}.share'.format(mining_algo)
        self.producer = self.initialize_kafka()

    def initialize_kafka(self):
        producer = Producer(brokers=self.servers)
        return producer

    @coroutine
    def submit_share(self, pool_type, worker_name, shares, nbits, is_valid):
        try:
            target = Utils.bits_to_target(nbits)
            pps_value = Utils.calculate_pps_value(MINING_ALGO, target, shares)
            share_data = {
                'pool_type': pool_type,
                'blockchain': self.blockchain,
                'worker_name': worker_name,
                'shares': shares,
                'is_valid': is_valid,
                'timestamp': int(time.time()),
                'ip': '',
                'height': 0,
                'pps_value': pps_value,
                'bits': nbits,
            }
            app_log.info('Submit shares to kafka:{}'.format(share_data))
            yield self.producer.produce(self.topic, share_data)
        except:
            app_log.info(format_exc())
        return True
