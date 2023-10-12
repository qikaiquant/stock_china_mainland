import getopt
import importlib
import os
import sys
from enum import Enum

sys.path.append(os.path.dirname(sys.path[0]))
from utils.db_tool import *
from utils.redis_tool import *
from multiprocessing import Process, Lock


class Param_Status(Enum):
    NEW = 0
    CHECKING = 1
    FINISHED = 2


def search_param(tid, dbtool, lo):
    while True:
        lo.acquire()
        res = dbtool.get_param(Param_Status.NEW.value, 1)
        if res is None:
            logging.error("Process " + str(tid) + " CANNOT Fetch New Param, Exit")
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


def init_strategy():
    stg_id = conf_dict['Backtest']['STG']
    if stg_id not in conf_dict['STG']:
        logging.error("No " + stg_id + " Settled in STG Seg.")
        sys.exit(1)
    file = importlib.import_module(conf_dict['STG'][stg_id]['Module_Path'])
    cls = getattr(file, conf_dict['STG'][stg_id]['Class_Name'])
    # 初始化存储连接
    dbtool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                    conf_dict['Mysql']['Passwd'])
    cache_tool = RedisTool(conf_dict['Redis']['Host'], conf_dict['Redis']['Port'], conf_dict['Redis']['Passwd'])
    # 新建实例
    return cls(conf_dict['Backtest']['Start_Date'], conf_dict['Backtest']['End_Date'], dbtool, cache_tool, stg_id)


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
            str_param = pid.split("_")
            param = []
            for p in str_param:
                param.append(int(p))
            stg.reset_param(param)
            stg.backtest(pid)
        elif opt == '--refresh-param-space':
            # 重刷参数空间分支
            stg = init_strategy()
            ps = stg.build_param_space()
            stg.db_tool.refresh_param_space(ps)
        elif opt == '--search-param':
            # 搜参分支
            process_num = conf_dict['Backtest']['Search_Param_Process_Num']
            db_tool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                             conf_dict['Mysql']['Passwd'])
            process_list = []
            lock = Lock()
            for i in range(0, process_num):
                t = Process(target=search_param, args=(i, db_tool, lock))
                t.start()
                process_list.append(t)
            for i in process_list:
                i.join()
            logging.info("Main Process Finished.")
        else:
            logging.error("Usage Error")
            sys.exit(1)
    logging.info("End Backtest")
