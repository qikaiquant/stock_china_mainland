import getopt
import os
import sys

import pandas
import talib
import numpy
import traceback as tb

sys.path.append(os.path.dirname(sys.path[0]))
from utils.db_tool import *
from utils.redis_tool import *
from utils.common import *

Stock_DB_Tool = None
Stock_Redis_Tool = None


class PreHandlers:
    @staticmethod
    def ph_macd(db_no):
        Stock_Redis_Tool.clear(int(db_no))
        stocks = Stock_DB_Tool.get_stock_info(['stock_id'])
        cols = ['dt', 'close', 'avg', 'money']
        for (stock_id,) in stocks:
            try:
                res = Stock_DB_Tool.get_price(stock_id, fields=cols)
                # 股票在2013-01-01前已退市
                if len(res) == 0:
                    continue
                res_df = pandas.DataFrame(res, columns=cols)
                res_df.set_index('dt', inplace=True)
                res_df['dif'], res_df['dea'], res_df['hist'] = talib.MACD(numpy.array(res_df['close']), fastperiod=12,
                                                                          slowperiod=26, signalperiod=9)
                Stock_Redis_Tool.set(stock_id, res_df, db_no, serialize=True)
            except Exception as e:
                tb.print_exc()
                print(stock_id)
                break


def warm_db(stg_map, s_list):
    for s in s_list:
        if s not in stg_map:
            logging.warning("STG " + s + " NOT in STG Config.Please CHECK config.json")
            continue
        if ("PreHandler" not in stg_map[s]) or (not hasattr(PreHandlers, stg_map[s]['PreHandler'])):
            logging.warning(s + " Has NO PreHandler.Please CHECK config.json")
            continue
        func_str = stg_map[s]['PreHandler']
        db_no = stg_map[s]['DB_NO']
        logging.info("To Warm Cache for PreHandler " + func_str + " In DB " + str(db_no))
        func = getattr(PreHandlers, func_str)
        func(db_no)


if __name__ == '__main__':
    logging.info("Start Warmer")
    # 初始化数据库
    Stock_DB_Tool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                           conf_dict['Mysql']['Passwd'])
    # 初始化Redis
    Stock_Redis_Tool = RedisTool(conf_dict['Redis']['Host'], conf_dict['Redis']['Port'], conf_dict['Redis']['Passwd'])
    # 预热缓存
    cache_map = conf_dict['STG']
    opts, args = getopt.getopt(sys.argv[1:], "an:")
    for k, v in opts:
        # 预热所有db
        if k == '-a':
            warm_db(cache_map, cache_map.keys())
        # 预热指定db
        elif k == '-n':
            stgs = v.split(',')
            stg_list = []
            for stg in stgs:
                stg_list.append(stg.strip())
            warm_db(cache_map, stg_list)
        else:
            logging.error("Usage Error")
            sys.exit(1)
    logging.info("Warmer End")
