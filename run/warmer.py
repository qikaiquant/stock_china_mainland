import sys
import os

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
    for (stock_id,) in res:
        res = Stock_DB_Tool.get_price(stock_id, fields=db_col)
        Stock_Redis_Tool.set_price(stock_id, res)


if __name__ == '__main__':
    # 读配置文件
    conf_dict = load_config("../config/config.ini")
    # 初始化数据库
    Stock_DB_Tool = DBTool(conf_dict['Mysql']['host'], conf_dict['Mysql']['port'], conf_dict['Mysql']['user'],
                           conf_dict['Mysql']['passwd'])
    # 初始化Redis
    Stock_Redis_Tool = RedisTool(conf_dict['Redis']['host'], conf_dict['Redis']['port'], conf_dict['Redis']['passwd'],
                                 0)
    # 预热缓存
    log("Start Cache Warmer")
    load_price()
