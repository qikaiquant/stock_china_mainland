import traceback as tb
import configparser as cp

import sys
import os

sys.path.append(os.path.dirname(sys.path[0]))

import getopt
import utils.db_tool
import utils.misc
import datetime

from jqdatasdk import *
from datetime import timedelta

JK_User = None
JK_Token = None

Stock_DB_Tool = None

TBF_Dir = None
File_Locked = 'TBF.Locked'
File_TBF = 'price.TBF.txt'


def _get_trade_days():
    auth(JK_User, JK_Token)
    Stock_DB_Tool.insert_trade_days(get_all_trade_days())
    logout()


def _get_all_stock_info():
    auth(JK_User, JK_Token)
    Stock_DB_Tool.insert_stock_info(pd.DataFrame(get_all_securities(['stock'])))
    logout()


def _scan():
    if not os.path.exists(TBF_Dir):
        os.mkdir(TBF_Dir)
    os.chdir(TBF_Dir)
    # 确认锁
    if os.path.exists(File_Locked):
        print("Last Round NOT Finished,Exit")
        return
    # 只能在Linux上运行
    os.mknod(File_Locked)
    # 开始Scan，目前只有Daily抓取
    fp = open(File_TBF, 'w')
    stocks = Stock_DB_Tool.get_stock_info(['stock_id', 'start_date'])
    for stock in stocks:
        stock_id = stock[0]
        ipo_date = stock[1]
        start_date = datetime.datetime.strptime('2013-01-01', '%Y-%m-%d').date()
        if ipo_date > start_date:
            start_date = ipo_date
        tds = set(Stock_DB_Tool.get_trade_days(str(start_date), str(datetime.date.today())))
        suffix = utils.misc.stockid2table(stock_id, 10)
        table_name = "quant_stock.price_daily_r" + str(suffix)
        sql = "select dt from " + table_name + " where sid = \'" + stock_id + '\''
        dt = set(Stock_DB_Tool.exec_raw_select(sql))
        tbf_fetch_list = tds - dt
        for tbf in tbf_fetch_list:
            print(stock_id + ',' + str(tbf[0]) + ',Daily', file=fp)
        print(stock_id + " Finished.")
    fp.close()
    # 释放锁
    if os.path.exists(File_Locked):
        os.remove(File_Locked)


def _fetch_price():
    if not os.path.exists(TBF_Dir):
        print("TBF Dir NOT Exit")
        return
    os.chdir(TBF_Dir)
    # 确认锁及抓取文件
    if os.path.exists(File_Locked) or not os.path.exists(File_TBF):
        print("Last Round NOT Finished OR NO TBF,Exit")
        return
    # 只能在Linux上运行
    # os.mknod(File_Locked)
    # 取行情
    fp = None
    try:
        fp = open(File_TBF, 'r')
        line = fp.readline()
        fetch_map = {}
        while line:
            segs = line.split(',')
            stock_id = segs[0]
            dt = datetime.datetime.strptime(segs[1], '%Y-%m-%d').date()
            freq = segs[2]
            line = fp.readline()
            if stock_id in fetch_map:
                fetch_map[stock_id].append(dt)
            else:
                fetch_map[stock_id] = [dt]
        print("Load Fetch File Done.")
        for stock_id, dts in fetch_map.items():
            list.sort(dts)
            time_segs = []
            if len(dts) == 0:
                print(stock_id, time_segs)
                break
            start_date = dts[0]
            for i in range(1, len(dts)):
                if dts[i] - dts[i - 1] < timedelta(days=15):
                    continue
                time_segs.append((start_date, dts[i - 1]))
                start_date = dts[i]
            time_segs.append((start_date, dts[-1]))
            print(stock_id, time_segs)
            break
    except:
        tb.print_exc()
    finally:
        fp.close()
        # 释放锁
        # os.remove(File_Locked)


def _check_spare():
    auth(JK_User, JK_Token)
    print(str(get_query_count()))
    logout()


if __name__ == '__main__':
    try:
        cf = cp.ConfigParser()
        cf.read("../config/config.ini")
        # 初始化数据库
        host = cf.get("Mysql", 'Host')
        port = int(cf.get("Mysql", 'Port'))
        user = cf.get("Mysql", 'User')
        passwd = cf.get("Mysql", 'Passwd')
        Stock_DB_Tool = utils.db_tool.DBTool(host, port, user, passwd)
        # 聚宽账号
        JK_User = cf.get("DataSource", 'JK_User')
        JK_Token = cf.get("DataSource", 'JK_Token')
        # 行情抓取相关配置
        TBF_Dir = cf.get("DataSource", "TBF_Dir")

        opts, args = getopt.getopt(sys.argv[1:], "",
                                   longopts=["trade_days", "all_stock_info", "spare", "scan", "price"])
        for opt, _ in opts:
            if opt == '--trade_days':
                # 获取所有交易日
                _get_trade_days()
            elif opt == '--all_stock_info':
                # 获取所有股票的基本信息
                _get_all_stock_info()
            elif opt == '--spare':
                # 获取api日限额余量
                _check_spare()
            elif opt == '--scan':
                # 获取待抓取的列表
                _scan()
            elif opt == '--price':
                # 抓取行情
                _fetch_price()
            else:
                raise getopt.GetoptError("Usage Error")
    except getopt.GetoptError:
        tb.print_exc()
    except:
        tb.print_exc()
