import pickle
import time

import redis


class RedisTool:
    def __init__(self, host, port, passwd, db):
        print("Redis Connection Init")
        self._conn = redis.Redis(host=host, port=port, password=passwd, db=db)

    def set_price(self, stock_id, res):
        res_bytes = pickle.dumps(res)
        t0 = time.time()
        self._conn.set(stock_id, res_bytes)
        t1 = time.time()
        return t0, t1

    def get_price(self, stock_id):
        res = self._conn.get(stock_id)
        return pickle.loads(res)

    def __del__(self):
        print("Redis Connection Closed.")
        self._conn.close()
