import getopt
import importlib
import os
import signal
import sys
from enum import Enum

sys.path.append(os.path.dirname(sys.path[0]))
from utils.db_tool import *
from utils.redis_tool import *
from multiprocessing import *

Parent_Conn_List = []


class Param_Status(Enum):
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
        res = dbtool.get_param(Param_Status.NEW.value, 1)
        if res is None:
            logging.fatal("Process " + str(tid) + " CANNOT Fetch New Param, Exit")
            lo.release()
            break
        pid = res[0][0]
        param = res[0][1]
        dbtool.updata_param_status(pid, Param_Status.CHECKING.value)
        lo.release()
        logging.info("Process " + str(tid) + " Get Param " + pid)
        s = init_strategy()
        s.reset_param(param)
        s.backtest(pid=pid)
        del s
        lo.acquire()
        dbtool.updata_param_status(pid, Param_Status.FINISHED.value)
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
    file = importlib.import_module(conf_dict['STG'][stg_id]['Module_Path'])
    cls = getattr(file, conf_dict['STG'][stg_id]['Class_Name'])
    # 初始化实例
    return cls(stg_id)


if __name__ == '__main__':
    logging.info("Start Backtest")
    opts, args = getopt.getopt(sys.argv[1:], "",
                               longopts=["survey", "backtest", "refresh-param-space", "search-param"])
    for opt, _ in opts:
        if opt == '--survey':
            # 调研分支
            stg = init_strategy()
            stg.survey([], False)
        elif opt == '--backtest':
            # 单回测分支
            stg = init_strategy()
            pid = "1_-1_27_19_2"
            stg.reset_param(pid2param(pid))
            stg.backtest(pid)
        elif opt == '--refresh-param-space':
            # 重刷参数空间分支
            stg = init_strategy()
            ps = stg.build_param_space()
            stg.db_tool.refresh_param_space(ps)
        elif opt == '--search-param':
            # 搜参分支
            p_id = os.getpid()
            with open("backtest.searchparam.pid", 'w') as file:
                file.write(str(p_id))
            lock = Lock()
            signal.signal(signal.SIGUSR1, notice_handler)
            db_tool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                             conf_dict['Mysql']['Passwd'])
            process_num = conf_dict["Backtest"]["Prceoss_Num"]
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
