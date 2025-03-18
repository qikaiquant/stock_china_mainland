import getopt
import importlib
from datetime import date

from trade.qmt_trader import QMTTrader
from utils.common import *


def init_strategy(real_flag):
    stg_id = conf_dict['Trade']['STG']
    if stg_id not in conf_dict['STG']:
        logging.error("No " + stg_id + " Settled in STG Seg.")
        sys.exit(1)
    f = importlib.import_module(conf_dict['STG'][stg_id]['Module_Path'])
    cls = getattr(f, conf_dict['STG'][stg_id]['Class_Name'])
    # 初始化实例
    trader = QMTTrader(conf_dict['STG']['Base']["TotalBudget"], conf_dict['STG']['Base']["MaxHold"], real_flag)
    inst = cls(stg_id, trader)
    return inst


if __name__ == "__main__":
    logging.info("Start Hunter")
    opts, args = getopt.getopt(sys.argv[1:], "", longopts=["prepare", "trade", "real"])
    # 提取real_flag,是否实盘
    real_flag = False
    for opt, arg in opts:
        if opt == '--real':
            real_flag = True
    # 如果是实盘，只在交易日运行
    stg = init_strategy(real_flag)
    td = date.today()
    if real_flag:
        all_trade_days = stg.db_tool.get_trade_days()
        if td not in all_trade_days:
            logging.error("NOT A Trade Day,QUIT!")
            sys.exit(-1)
    for opt, arg in opts:
        # 开市前准备
        if opt == '--prepare':
            logging.info("Prepare")
            stg.pre_market_action(td)
        # 交易
        elif opt == '--trade':
            logging.info("Trade")
            stg.market_action(td)
    logging.info("End Hunter")
