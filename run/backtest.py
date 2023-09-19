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
    cache_no = conf_dict['STG'][stg_id]['DB_NO']
    # 初始化存储连接
    db_tool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                     conf_dict['Mysql']['Passwd'])
    cache_tool = RedisTool(conf_dict['Redis']['Host'], conf_dict['Redis']['Port'], conf_dict['Redis']['Passwd'])
    # 开始回测
    stg = cls(conf_dict['Backtest']['Start_Date'], conf_dict['Backtest']['End_Date'], db_tool, cache_tool, cache_no,
              conf_dict['Backtest']['Budget'], conf_dict['Backtest']['MaxHold'])
    stg.run()
    logging.info("End Backtest")
