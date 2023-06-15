import pickle
import time

import redis


class RedisTool:
    def __init__(self, host, port, passwd, db):
        print("Redis Connection Init")
        self._conn = redis.Redis(host=host, port=port, password=passwd, db=db)

    def set_price(self, stock_id, res):
        self._conn.set(stock_id, pickle.dumps(res))

    def get_price(self, stock_id):
        res = self._conn.get(stock_id)
        return pickle.loads(res)

    def __del__(self):
        print("Redis Connection Closed.")
        self._conn.close()
