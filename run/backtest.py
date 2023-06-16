import os
import sys
import importlib

sys.path.append(os.path.dirname(sys.path[0]))

from strategy.base_strategy import STGContext
from utils.db_tool import *
from utils.redis_tool import *

if __name__ == '__main__':
    logging.info("Start Backtest")
    file = importlib.import_module(conf_dict['Backtest']['strategy_module_path'])
    cls = getattr(file, conf_dict['Backtest']['strategy_class_name'])
    # 初始化存储连接
    db_tool = DBTool(conf_dict['Mysql']['host'], conf_dict['Mysql']['port'], conf_dict['Mysql']['user'],
                     conf_dict['Mysql']['passwd'])
    cache_tool = RedisTool(conf_dict['Redis']['host'], conf_dict['Redis']['port'], conf_dict['Redis']['passwd'])
    # 开始回测
    stg = cls(
        STGContext(conf_dict['Backtest']['btc_start_date'], conf_dict['Backtest']['btc_end_date'], db_tool, cache_tool))
    stg.backtest()
    logging.info("End Backtest")
