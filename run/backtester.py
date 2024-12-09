import getopt
import importlib
import os
import signal
import sys
import pandas
from sqlalchemy.testing.plugin.plugin_base import logging

sys.path.append(os.path.dirname(sys.path[0]))

from trade.trader import Backtest_Trader
from utils.db_tool import *
from utils.redis_tool import *
from multiprocessing import *

Parent_Conn_List = []


class ParamStatus(Enum):
    NEW = 0
    CHECKING = 1
    FINISHED = 2


def notice_handler(sig, stack):
    logging.fatal("Get Exit Signal, Notify SubProcess:")
    # 通知子进程优雅退出
    for c in Parent_Conn_List:
        c.send(1)


def search_param(tid, dbtool, lo, conn):
    while True:
        lo.acquire()
        res = dbtool.get_param(ParamStatus.NEW.value, 1)
        if res is None:
            logging.fatal("Process " + str(tid) + " CANNOT Fetch New Param, Exit")
            lo.release()
            break
        pid = res[0][0]
        dbtool.updata_param_status(pid, ParamStatus.CHECKING.value)
        lo.release()
        logging.info("Process " + str(tid) + " Get Param " + pid)
        s = init_strategy()
        backtest(s, pid)
        del s
        lo.acquire()
        dbtool.updata_param_status(pid, ParamStatus.FINISHED.value)
        lo.release()
        logging.info("Process " + str(tid) + " Finish Param " + pid)
        if conn.poll():
            conn.recv()
            logging.fatal("Get Exit Signal.Process " + str(tid) + " Exit.")
            break


def init_strategy():
    stg_id = conf_dict['Backtest']['STG']
    if stg_id not in conf_dict['STG']:
        logging.error("No " + stg_id + " Settled in STG Seg.")
        sys.exit(1)
    f = importlib.import_module(conf_dict['STG'][stg_id]['Module_Path'])
    cls = getattr(f, conf_dict['STG'][stg_id]['Class_Name'])
    # 初始化实例
    inst = cls(stg_id)
    inst.position.set_trader(Backtest_Trader())
    return inst


def backtest(stg, pid=""):
    bt_start_date = conf_dict['Backtest']['Start_Date']
    bt_end_date = conf_dict['Backtest']['End_Date']
    # 设置benchmark
    daily_benchmark = pandas.DataFrame()
    for bm in BenchMark:
        res = stg.db_tool.get_price(bm.value, ['dt', 'close'], bt_start_date, bt_end_date)
        factor = float(conf_dict['STG']['Base']['TotalBudget'] / res[0][1])
        bmdf = pandas.DataFrame(res, columns=['dt', 'jiage'])
        daily_benchmark['dt'] = bmdf['dt']
        daily_benchmark[bm.name] = bmdf['jiage'] * factor
    daily_benchmark.set_index('dt', inplace=True)
    stg.cache_tool.set(BENCHMARK_KEY, daily_benchmark, COMMON_CACHE_ID, serialize=True)
    # 刷参数
    if pid != "":
        stg.reset_param(pid2param(pid))
    # 遍历所有回测交易日
    bt_tds = stg.db_tool.get_trade_days(bt_start_date, bt_end_date)
    for dt in bt_tds:
        logging.info("+++++++++++++++++++" + str(dt) + "++++++++++++++++++++")
        # 调整仓位
        stg.adjust_position(dt)


if __name__ == '__main__':
    logging.info("Start Backtest")
    opts, args = getopt.getopt(sys.argv[1:], "",
                               longopts=["refresh-param-space", "survey", "single", "search-param"])
    for opt, arg in opts:
        if opt == '--refresh-param-space':
            # 重刷参数空间分支
            stg = init_strategy()
            ps = stg.build_param_space()
            stg.db_tool.refresh_param_space(ps)
        elif opt == '--survey':
            # 调研分支
            stg = init_strategy()
            stg.survey()
        elif opt == '--single':
            # 单回测分支
            stg = init_strategy()
            backtest(stg, "")
        elif opt == '--search-param':
            # 搜参分支
            p_id = os.getpid()
            with open("backtest.searchparam.pid", 'w') as file:
                file.write(str(p_id))
            lock = Lock()
            signal.signal(signal.SIGUSR1, notice_handler)
            db_tool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                             conf_dict['Mysql']['Passwd'])
            process_num = conf_dict["Backtest"]["Process_Num"]
            if process_num == -1:
                process_num = cpu_count()
            for i in range(0, process_num):
                (p_conn, c_conn) = Pipe()
                t = Process(target=search_param, args=(i, db_tool, lock, c_conn))
                t.start()
                Parent_Conn_List.append(p_conn)
            signal.pause()
            os.remove("backtest.searchparam.pid")
        else:
            logging.error("Usage Error")
            sys.exit(1)
    logging.info("End Backtest")
