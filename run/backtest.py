import getopt
import os
import sys
import importlib

sys.path.append(os.path.dirname(sys.path[0]))
from utils.db_tool import *
from utils.redis_tool import *

if __name__ == '__main__':
    logging.info("Start Backtest")
    stg_id = conf_dict['Backtest']['STG']
    if stg_id not in conf_dict['STG']:
        logging.error("No " + stg_id + " Settled in STG Seg.")
        sys.exit(1)
    file = importlib.import_module(conf_dict['STG'][stg_id]['Module_Path'])
    cls = getattr(file, conf_dict['STG'][stg_id]['Class_Name'])
    # 初始化存储连接
    db_tool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                     conf_dict['Mysql']['Passwd'])
    cache_tool = RedisTool(conf_dict['Redis']['Host'], conf_dict['Redis']['Port'], conf_dict['Redis']['Passwd'])
    # 开始回测
    stg = cls(conf_dict['Backtest']['Start_Date'], conf_dict['Backtest']['End_Date'], db_tool, cache_tool,
              conf_dict['STG'][stg_id]['DB_NO'])
    opts, args = getopt.getopt(sys.argv[1:], "",
                               longopts=["survey", "backtest", "param-search"])
    for opt, _ in opts:
        if opt == '--survey':
            # 调研分支
            stg.survey([], False)
        elif opt == '--backtest':
            # 单回测分支
            stg.backtest()
        elif opt == '--param-search':
            # 搜参分支
            pass
        else:
            logging.error("Usage Error")
            sys.exit(1)
    logging.info("End Backtest")
