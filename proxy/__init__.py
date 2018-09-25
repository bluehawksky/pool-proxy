# coding=utf-8

from tornado.gen import coroutine
from tornado.log import app_log
from lib.kafka_util import KafkaUtil
from proxy.connection import WorkerConnection
from config import KAFKA_SERVER, BLOCKCHAIN_NAME, MINING_ALGO


@coroutine
def init_kafka():
    kafka_producer = KafkaUtil(KAFKA_SERVER, BLOCKCHAIN_NAME, MINING_ALGO)
    WorkerConnection.kafka_producer = kafka_producer
    yield kafka_producer.producer.connect()
    app_log.info('Start kafka service')


@coroutine
def stop_kafka():
    try:
        yield WorkerConnection.kafka_producer.producer.wind_down()
        app_log.info('Stop kafka service')
    except:
        pass
