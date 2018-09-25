# coding=utf-8


class ErrorCode:
    INVALID_MINER_NAME = 1001
    INVALID_POOL_ADDRESS = 1002
    CONNECT_LOST = 1101
    AUTH_FAILED = 1102


ErrorMsg = {
    ErrorCode.INVALID_MINER_NAME: "Invalid miner name",
    ErrorCode.INVALID_POOL_ADDRESS: "Invalid pool address",

    ErrorCode.CONNECT_LOST: "Connection lost",
    ErrorCode.AUTH_FAILED: "Authorize failed"
}