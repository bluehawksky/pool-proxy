# coding=utf-8

from tornado.gen import coroutine
from lib.util import Utils
from config import AUTH_URL, MINING_ALGO


@coroutine
def miner_auth(miner_name):
    try:
        response = yield Utils.fetch_url(AUTH_URL.format(miner_name, MINING_ALGO))
        return response
    except:
        return {}
