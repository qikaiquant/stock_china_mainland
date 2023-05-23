import datetime as dt
import traceback as tb
import configparser as cp
import pandas as pd

import sys
import time
import getopt
import datetime
import numpy
import utils.db_tool

from jqdatasdk import *
from utils.misc import *

JK_User = None
JK_Token = None

Msg_Base_Url = None
Stock_DB_Tool = None


def get_stock_trade_days():
    auth(JK_User, JK_Token)
    Stock_DB_Tool.insert_trade_days(get_all_trade_days())
    logout()


def get_all_stocks_info():
    auth(JK_User, JK_Token)
    Stock_DB_Tool.insert_stock_info(pd.DataFrame(get_all_securities(['stock'])))
    logout()


def _output_stock_price(code, pi):
    ok = True
    fp = None
    try:
        fp = open(Price_Dir + code.replace(".", "_") + ".txt", 'w')
        print('code\topen\tclose\tlow\thigh\tvolume\tmoney\tfactor\thigh_limit\tlow_limit\tavg\tpre_close\tpaused',
              file=fp)
        for index, row in pi.iterrows():
            outstr = str(index) + "\t" + str(row["open"]) + "\t" + str(row["close"]) + "\t" + str(
                row["low"]) + "\t" + str(row[
                                             "high"]) + "\t" + str(row["volume"]) + "\t" + str(
                row["money"]) + "\t" + str(row["factor"]) + "\t" + str(row[
                                                                           "high_limit"]) + "\t" + \
                     str(row["low_limit"]) + "\t" + str(row["avg"]) + "\t" + str(row["pre_close"]) + "\t" + str(
                row["paused"])
            print(outstr, file=fp)
    except:
        tb.print_exc()
        ok = False
    finally:
        if fp is not None:
            fp.close()
    return ok


def _load_finished(cursor_file):
    finished_set = set()
    fp_cursor = None

    try:
        fp_cursor = open(cursor_file, 'r')
        while True:
            line = fp_cursor.readline()
            if not line:
                break
            code = line.strip().split(' ')[1]
            finished_set.add(code)
    except:
        tb.print_exc()
    finally:
        if fp_cursor is not None:
            fp_cursor.close()
    return finished_set


def get_stock_price():
    fp = None
    fp_cursor = None

    formatstr = '%Y-%m-%d %H:%M:%S'
    end_date = dt.datetime.strptime(Price_End_Date, formatstr)
    finished_set = _load_finished(Cursor_File)

    msg_dict = {"Su_OK": 0, "Su_WARN": 0, "Success": 0, "Error": 0, "Except": False}

    try:
        fp = open(All_Stocks_File, 'r')
        fp_cursor = open(Cursor_File, 'a+')
        auth(JK_User, JK_Token)
        while True:
            line = fp.readline()
            if not line:
                break
            infos = line.strip().split(',')
            code = infos[0]
            ipo = infos[3]
            if code in finished_set:
                continue
            ipo_date = dt.datetime.strptime(ipo, formatstr)
            start_date = dt.datetime.strptime(Price_Start_Date, formatstr)
            if start_date < ipo_date:
                start_date = ipo_date
            tdc = len(get_trade_days(start_date, end_date))
            pi = get_price(code, end_date=end_date, start_date=start_date, frequency='daily',
                           fields=['open', 'close', 'low', 'high', 'volume', 'money', 'factor', 'high_limit',
                                   'low_limit', 'avg', 'pre_close', 'paused'])
            pc = len(pi)
            if not _output_stock_price(code, pd.DataFrame(pi)):
                print("Output Error For Code :", code)
                msg_dict['Error'] += 1
                break
            check_flag = None
            if pc != tdc:
                check_flag = 'WARN'
                msg_dict['Su_WARN'] += 1
            else:
                check_flag = 'OK'
                msg_dict["Su_OK"] += 1
            msg_dict["Success"] += 1
            print("Code " + code + " Finished[" + str(pc) + "L," + str(tdc) + "D," + check_flag + "]", file=fp_cursor)
            time.sleep(1)
    except Exception as e:
        if str(e.args).find("您当天的查询条数超过了每日最大查询限制") != -1:
            print("Rearch Daily Limited.")
        else:
            tb.print_exc()
            msg_dict['Except'] = True
    finally:
        if fp is not None:
            fp.close()
        if fp_cursor is not None:
            fp_cursor.close()
        send_wechat_message("每日行情抓取详情", str(msg_dict))
        logout()


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

        All_Stocks_File = cf.get("DataSource", 'All_Stocks_File')
        Cursor_File = cf.get("DataSource", 'Cursor_File')
        Price_Dir = cf.get("DataSource", 'Price_Dir')
        Price_Start_Date = cf.get("DataSource", 'Price_Start_Date')
        Price_End_Date = cf.get("DataSource", 'Price_End_Date')

        opts, args = getopt.getopt(sys.argv[1:], "tap:")
        for opt, v in opts:
            if opt == '-t':
                # 获取所有交易日
                get_stock_trade_days()
            elif opt == '-a':
                # 获取所有股票的基本信息
                get_all_stocks_info()
            elif opt == '-p':
                print("Price For", v)
            else:
                raise getopt.GetoptError
    except getopt.GetoptError:
        tb.print_exc()
    except:
        tb.print_exc()
