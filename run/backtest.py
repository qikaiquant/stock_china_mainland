import sys
import os

sys.path.append(os.path.dirname(sys.path[0]))
import importlib
from strategy.base_strategy import STGContext
from utils.common import *
from utils.db_tool import *

if __name__ == '__main__':
    conf_dict = load_config("../config/config.ini")
    file = importlib.import_module(conf_dict['Backtest']['strategy_module_path'])
    cls = getattr(file, conf_dict['Backtest']['strategy_class_name'])
    # 初始化数据库连接
    db_tool = DBTool(conf_dict['Mysql']['host'], conf_dict['Mysql']['port'], conf_dict['Mysql']['user'],
                     conf_dict['Mysql']['passwd'])
    # 开始回测
    stg = cls(STGContext(conf_dict['Backtest']['btc_start_date'], conf_dict['Backtest']['btc_end_date'], db_tool))
    stg.backtest()
