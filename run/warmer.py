import getopt
import logging
import os
import sys

import pandas

sys.path.append(os.path.dirname(sys.path[0]))
from utils.db_tool import *
from utils.redis_tool import *
from utils.common import *

Stock_DB_Tool = None
Stock_Redis_Tool = None


class PreHandlers:
    @staticmethod
    def ph_macd(db_no):
        Stock_Redis_Tool.clear(db_no)
        res = Stock_DB_Tool.get_stock_info(['stock_id'])
        cols = ['dt', 'close', 'avg', 'money']
        for (stock_id,) in res:
            res = Stock_DB_Tool.get_price(stock_id, fields=cols)
            res_df = pandas.DataFrame(res, columns=cols)
            res_df.set_index('dt', inplace=True)
            Stock_Redis_Tool.set(stock_id, res_df, db_no, serialize=True)


def warm_db(c_map, dblist):
    for db_no in dblist:
        db_no_str = str(db_no)
        if db_no_str not in c_map:
            logging.warning("db_no " + str(db_no_str) + " NOT in DB_No_Map, CHECK Config.ini")
            continue
        func_str = c_map[db_no_str]
        if not hasattr(PreHandlers, func_str):
            logging.warning(func_str + " is NOT Set in PreHandlers,CHECK Config.ini")
            continue
        logging.info("To Warm Cache for PreHandler " + func_str + " In DB " + db_no_str)
        func = getattr(PreHandlers, func_str)
        func(db_no)


if __name__ == '__main__':
    logging.info("Start Warmer")
    # 初始化数据库
    Stock_DB_Tool = DBTool(conf_dict['Mysql']['host'], conf_dict['Mysql']['port'], conf_dict['Mysql']['user'],
                           conf_dict['Mysql']['passwd'])
    # 初始化Redis
    Stock_Redis_Tool = RedisTool(conf_dict['Redis']['host'], conf_dict['Redis']['port'], conf_dict['Redis']['passwd'])
    # 预热缓存
    cache_map = conf_dict['Redis']['db_no_map']
    opts, args = getopt.getopt(sys.argv[1:], "an:")
    for k, v in opts:
        # 预热所有db
        if k == '-a':
            warm_db(cache_map, cache_map.keys())
        # 预热指定db
        elif k == '-n':
            dbs = v.split(',')
            db_list = []
            for db in dbs:
                db_list.append(int(db.strip()))
            warm_db(cache_map, db_list)
        else:
            logging.error("Usage Error")
            sys.exit(1)
    logging.info("Warmer End")
