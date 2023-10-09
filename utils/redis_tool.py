import logging
import redis
import pickle


class RedisTool:
    def __init__(self, host, port, passwd):
        logging.info("Redis Init")
        self._conn = redis.Redis(host=host, port=port, password=passwd)

    def clear(self, db_no):
        self._conn.select(db_no)
        self._conn.flushdb()

    def set(self, key, value, db_no, serialize=False):
        self._conn.select(db_no)
        if serialize:
            self._conn.set(key, pickle.dumps(value))
        else:
            self._conn.set(key, value)

    def get(self, key, db_no, serialize=False):
        self._conn.select(db_no)
        res = self._conn.get(key)
        if not res:
            return None
        if serialize:
            return pickle.loads(res)
        else:
            return res

    def get_keys(self, db_no, pattern):
        self._conn.select(db_no)
        res = []
        for key in self._conn.scan_iter(pattern):
            res.append(key)
        return res

    def delete(self, key, db_no):
        self._conn.select(db_no)
        self._conn.delete(key)

    def __del__(self):
        self._conn.close()
