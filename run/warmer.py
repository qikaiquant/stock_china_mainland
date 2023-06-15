import sys
import os
import configparser as cp
import time

sys.path.append(os.path.dirname(sys.path[0]))

from utils.db_tool import *
from utils.redis_tool import *
from utils.misc import *

Stock_DB_Tool = None
Stock_Redis_Tool = None


def load_price():
    res = Stock_DB_Tool.get_stock_info(['stock_id'])
    db_col = ['dt', 'open', 'close', 'low', 'high', 'volumn', 'money', 'factor',
              'high_limit', 'low_limit', 'avg', 'pre_close', 'paused']
    count = 1
    for (stock_id,) in res:
        t0 = time.time()
        res = Stock_DB_Tool.get_price(stock_id, fields=db_col)
        t1 = time.time()
        t2, t3 = Stock_Redis_Tool.set_price(stock_id, res)
        print("总耗时:" + str(t3 - t0))
        print("--读数据库:" + str(t1 - t0))
        print("--序列化:" + str(t2 - t1))
        print("--写Redis:" + str(t3 - t2))
        count += 1
        log(count)
        if count == 5:
            break


if __name__ == '__main__':
    # 读配置文件
    cf = cp.ConfigParser()
    cf.read("../config/config.ini")
    # 初始化数据库
    db_host = cf.get("Mysql", 'Host')
    db_port = int(cf.get("Mysql", 'Port'))
    db_user = cf.get("Mysql", 'User')
    db_passwd = cf.get("Mysql", 'Passwd')
    Stock_DB_Tool = DBTool(db_host, db_port, db_user, db_passwd)
    # 初始化Redis
    redis_host = cf.get("Redis", 'Host')
    redis_port = int(cf.get("Redis", 'Port'))
    redis_passwd = cf.get("Redis", 'Passwd')
    Stock_Redis_Tool = RedisTool(redis_host, redis_port, redis_passwd, 0)
    log("Start Cache Warmer")
    load_price()
