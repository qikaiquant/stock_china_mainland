import logging
import redis
import pickle

from enum import Enum


class db(Enum):
    DB_PRICE = 0
    DB_INFO = 1


class RedisTool:
    def __init__(self, host, port, passwd):
        logging.info("Redis Init")
        self._conn = redis.Redis(host=host, port=port, password=passwd)

    def clear(self, db_no):
        if not isinstance(db_no, db):
            logging.error("db_no MUST be db(Enum)")
            return
        self._conn.select(db_no.value)
        self._conn.flushdb()

    def set(self, key, value, db_no, serialize=False):
        if not isinstance(db_no, db):
            logging.error("db_no MUST be db(Enum)")
            return
        self._conn.select(db_no.value)
        if serialize:
            self._conn.set(key, pickle.dumps(value))
        else:
            self._conn.set(key, value)

    def get(self, key, db_no, serialize=False):
        if not isinstance(db_no, db):
            logging.error("db_no MUST be db(Enum)")
            return
        self._conn.select(db_no.value)
        res = self._conn.get(key)
        if serialize:
            return pickle.loads(res)
        else:
            return res

    def __del__(self):
        logging.info("Redis Closed.")
        self._conn.close()
