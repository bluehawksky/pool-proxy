#!/usr/bin/env python

import argparse
import logging
import random
import time


from tornado import gen, ioloop

from kiel.clients import Producer



def murmur2(data):
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



def key_maker(msg):
    return msg["worker_name"].split('.')[0]


def partitioner(key, partitions):

    idx = (murmur2(key) & 0x7fffffff) % len(partitions)
    print(key, idx)
    return idx



@gen.coroutine
def run(p):

    yield p.connect()

    while True:
        for i in range(10):
            yield p.produce(
                'sha256.share', {"worker_name": 'test{}.001'.format(i)}
            )
            time.sleep(1)

def main():
    loop = ioloop.IOLoop.instance()

    p = Producer(
        brokers=["172.16.3.210:9092","172.16.3.214:9092","172.16.3.215:9092"],
        key_maker=key_maker,
        partitioner=partitioner
    )
    loop.add_callback(run, p)

    try:
        loop.start()
    except KeyboardInterrupt:
        loop.stop()


if __name__ == "__main__":
    main()
