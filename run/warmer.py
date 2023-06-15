import sys
import os
import configparser as cp
import time
import platform

sys.path.append(os.path.dirname(sys.path[0]))

from utils.db_tool import *
from utils.redis_tool import *
from utils.misc import *

Stock_DB_Tool = None
Stock_Redis_Tool = None
OS_TYPE = platform.system()


def load_price():
    res = Stock_DB_Tool.get_stock_info(['stock_id'])
    db_col = ['dt', 'open', 'close', 'low', 'high', 'volumn', 'money', 'factor',
              'high_limit', 'low_limit', 'avg', 'pre_close', 'paused']
    for (stock_id,) in res:
        res = Stock_DB_Tool.get_price(stock_id, fields=db_col)
        Stock_Redis_Tool.set_price(stock_id, res)


if __name__ == '__main__':
    # 读配置文件
    cf = cp.ConfigParser()
    cf.read("../config/config.ini")
    # 初始化数据库
    db_host = cf.get("Mysql", 'Host')
    if OS_TYPE == 'Linux':
        db_host = 'localhost'
    db_port = int(cf.get("Mysql", 'Port'))
    db_user = cf.get("Mysql", 'User')
    db_passwd = cf.get("Mysql", 'Passwd')
    Stock_DB_Tool = DBTool(db_host, db_port, db_user, db_passwd)
    # 初始化Redis
    redis_host = cf.get("Redis", 'Host')
    if OS_TYPE == 'Linux':
        redis_host = 'localhost'
    redis_port = int(cf.get("Redis", 'Port'))
    redis_passwd = cf.get("Redis", 'Passwd')
    Stock_Redis_Tool = RedisTool(redis_host, redis_port, redis_passwd, 0)
    log("Start Cache Warmer")
    load_price()
