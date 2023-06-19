import os
import sys

import pandas

sys.path.append(os.path.dirname(sys.path[0]))
from utils.db_tool import *
from utils.redis_tool import *
from utils.common import *

Stock_DB_Tool = None
Stock_Redis_Tool = None
Raw_Price_DB_Clumns = ['dt', 'open', 'close', 'low', 'high', 'volumn', 'money', 'factor',
                       'high_limit', 'low_limit', 'avg', 'pre_close', 'paused']


class PreHandlers:
    @staticmethod
    def add_macd(res):
        del_col = ['open', 'low', 'high', 'volumn', 'factor',
                   'high_limit', 'low_limit', 'pre_close', 'paused']
        for c in del_col:
            del res[c]
        res.set_index('dt', inplace=True)


def load_price():
    res = Stock_DB_Tool.get_stock_info(['stock_id'])
    Stock_Redis_Tool.clear(db.DB_PRICE)
    for (stock_id,) in res:
        res = Stock_DB_Tool.get_price(stock_id, fields=Raw_Price_DB_Clumns)
        res_df = pandas.DataFrame(res, columns=Raw_Price_DB_Clumns)
        for prehs_str in conf_dict['Redis']['prehandlers']:
            if not hasattr(PreHandlers, prehs_str):
                logging.warning(prehs_str + " is NOT Set in PreHandlers")
                continue
            func = getattr(PreHandlers, prehs_str)
            func(res_df)
        Stock_Redis_Tool.set(stock_id, res_df, db.DB_PRICE, serialize=True)


if __name__ == '__main__':
    logging.info("Start Warmer")
    # 初始化数据库
    Stock_DB_Tool = DBTool(conf_dict['Mysql']['host'], conf_dict['Mysql']['port'], conf_dict['Mysql']['user'],
                           conf_dict['Mysql']['passwd'])
    # 初始化Redis
    Stock_Redis_Tool = RedisTool(conf_dict['Redis']['host'], conf_dict['Redis']['port'], conf_dict['Redis']['passwd'])
    # 预热缓存
    load_price()
    logging.info("Warmer End")
