import getopt
import importlib
import os
import sys
import pandas

sys.path.append(os.path.dirname(sys.path[0]))

from trade.trader import BacktestTrader
from utils.db_tool import *
from utils.redis_tool import *


def init_strategy():
    stg_id = conf_dict['Backtest']['STG']
    if stg_id not in conf_dict['STG']:
        logging.error("No " + stg_id + " Settled in STG Seg.")
        sys.exit(1)
    f = importlib.import_module(conf_dict['STG'][stg_id]['Module_Path'])
    cls = getattr(f, conf_dict['STG'][stg_id]['Class_Name'])
    # 初始化实例
    trader = BacktestTrader(conf_dict['STG']['Base']["TotalBudget"], conf_dict['STG']['Base']["MaxHold"])
    inst = cls(stg_id, trader)
    return inst


def add_snapshot(snapshot, stg, dt):
    snapshot.loc[dt] = None
    position = stg.trader.position
    try:
        nw = position.spare
        for slot in position.hold:
            stock_id = slot[0]
            if stock_id is None:
                continue
            price = stg.cache_tool.get(stock_id, conf_dict['Backtest']['Backtest_DB_NO'], True)
            dt_jiage = price.loc[dt, 'close']
            if stock_id not in snapshot.columns:
                snapshot[stock_id] = ""
            snapshot.at[dt, stock_id] = (dt_jiage, slot[2])
            nw += (dt_jiage * slot[2])
        snapshot.at[dt, 'Spare'] = position.spare
        snapshot.at[dt, 'stg_nw'] = nw
    except Exception as e:
        logging.info("Add SnapShot Error")
        snapshot.drop([dt])


def backtest():
    bt_start_date = conf_dict['Backtest']['Start_Date']
    bt_end_date = conf_dict['Backtest']['End_Date']
    stg = init_strategy()
    # 设置benchmark
    daily_benchmark = pandas.DataFrame()
    for bm in BENCH_MARK:
        res = stg.db_tool.get_price(bm, ['dt', 'close'], bt_start_date, bt_end_date)
        factor = float(conf_dict['STG']['Base']['TotalBudget'] / res[0][1])
        bmdf = pandas.DataFrame(res, columns=['dt', 'jiage'])
        daily_benchmark['dt'] = bmdf['dt']
        daily_benchmark[bm] = bmdf['jiage'] * factor
    daily_benchmark.set_index('dt', inplace=True)
    stg.cache_tool.set(BENCHMARK_KEY, daily_benchmark, COMMON_CACHE_ID, serialize=True)
    # 遍历所有回测交易日
    daily_snapshot = pandas.DataFrame(columns=['Spare', 'stg_nw'])
    bt_tds = stg.db_tool.get_trade_days(bt_start_date, bt_end_date)
    for dt in bt_tds:
        logging.info("+++++++++++++++++++" + str(dt) + "++++++++++++++++++++")
        # 调整仓位
        stg.adjust_position(dt)
        # 记录持仓状态
        add_snapshot(daily_snapshot, stg, dt)
    stg.cache_tool.set(RES_KEY_PREFIX + stg.stg_id, daily_snapshot, COMMON_CACHE_ID, serialize=True)


if __name__ == '__main__':
    logging.info("Start Backtest")
    opts, args = getopt.getopt(sys.argv[1:], "",
                               longopts=["survey", "run"])
    for opt, arg in opts:
        if opt == '--survey':
            # 调研分支
            stg = init_strategy()
            stg.survey()
        elif opt == '--run':
            # 运行分支
            backtest()
        else:
            logging.error("Usage Error")
            sys.exit(1)
    logging.info("End Backtest")
