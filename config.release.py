# encoding: utf-8

# #############################################################################
SITE = 'BTCC POOL'
HTTP_AUTO_RETRY = 3  # HTTP请求重试次数

MULTI_PROCESS_ENABLE = True
HOST = '0.0.0.0'
PORT = 8888
# #############################################################################
# pool
POOL_MINER_NAME = 'btccpool1'
MINING_ALGO = 'sha256'
BLOCKCHAIN_NAME = 'bitcoin'
AUTH_URL = 'http://172.16.3.201:8000/api/pool/miners/{}/blockchains/{}'

# 默认转发地址
DEFAULT_FORWARD_URL = 'stratum.btccpool.com:3333'
# 默认矿池类型
DEFAULT_POOL_TYPE = 0
# #############################################################################
# kafka
KAFKA_SERVER = []
# #############################################################################
LOG_CONFIG = {
    'level': 'info',  # debug < info < warning < error
    'filename': 'proxy.log',
    'backups': 100,
}
# #############################################################################
# job
JOB_CLEAR_TIME = 300
SUBMIT_JOB_CLEAR_TIME = 60

