import configparser as cp
import importlib as il
import traceback as tb
import utils.db_tool as udb

from strategy.base_strategy import STGContext

if __name__ == '__main__':
    cf = cp.ConfigParser()
    cf.read("../config/config.ini")

    try:
        # 初始化策略配置
        file = il.import_module(cf.get("Backtest", "Strategy_Module_Path"))
        cls = getattr(file, cf.get("Backtest", "Strategy_Class_Name"))
        sdate = cf.get("Backtest", "BTC_Start_Date")
        edate = cf.get("Backtest", "BTC_End_Date")
        # 初始化数据库连接
        host = cf.get("Mysql", 'Host')
        port = int(cf.get("Mysql", 'Port'))
        user = cf.get("Mysql", 'User')
        passwd = cf.get("Mysql", 'Passwd')
        db_conn = udb.DBTool(host, port, user, passwd)
        # 初始化STGContext
        if (not sdate) or (not edate):
            print("BTC Start or End Date NO Set, Backtest Failed.")
            raise RuntimeError("Error")
        # 开始回测
        stg = cls(STGContext(sdate, edate, db_conn))
        stg.backtest()
    except:
        tb.print_exc()
