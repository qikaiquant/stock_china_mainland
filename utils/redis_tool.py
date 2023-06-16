import redis
import pickle
from enum import Enum


class db(Enum):
    DB_PRICE = 0
    DB_INFO = 1


class RedisTool:
    def __init__(self, host, port, passwd):
        print("Redis Connection Init")
        self._conn = redis.Redis(host=host, port=port, password=passwd)

    def clear(self, db_no):
        self._conn.select(db_no)
        self._conn.flushdb(db_no)

    def set(self, key, value, db_no, serialize=False):
        self._conn.select(db_no)
        if serialize:
            self._conn.set(key, pickle.dumps(value))
        else:
            self._conn.set(key, value)

    def get(self, key, db_no, serialize=False):
        self._conn.select(db_no)
        res = self._conn.get(key)
        if serialize:
            return pickle.loads(res)
        else:
            return res

    def __del__(self):
        print("Redis Connection Closed.")
        self._conn.close()
