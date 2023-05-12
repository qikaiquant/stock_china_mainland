import datetime as dt
import traceback as tb
import configparser as cp
import pandas as pd

import os
import time

from jqdatasdk import *
from urllib import request
from urllib.parse import urlencode

JK_User = None
JK_Token = None

All_Stocks_File = None
Cursor_File = None
Price_Dir = None

Price_Start_Date = None
Price_End_Date = None

Msg_Base_Url = None


def get_all_stocks_info():
    fp = None
    try:
        auth(JK_User, JK_Token)
        fp = open(All_Stocks_File, 'w')
        print("code,cn_name,name,date", file=fp)
        all_stocks = pd.DataFrame(get_all_securities(['stock'], '2023-05-05'))
        for index, row in all_stocks.iterrows():
            line = str(index) + "," + row['display_name'] + "," + row['name'] + "," + str(row['start_date'])
            print(line, file=fp)
        count = get_query_count()
        print(count)
    except:
        tb.print_exc()
    finally:
        if fp is not None:
            fp.close()
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
            print("Reach Daily Limited.")
        else:
            tb.print_exc()
            msg_dict['Except'] = True
    finally:
        if fp is not None:
            fp.close()
        if fp_cursor is not None:
            fp_cursor.close()
        _send_message("每日行情抓取详情", str(msg_dict))
        logout()


def _check_spare():
    auth(JK_User, JK_Token)
    print(str(get_query_count()))
    logout()


def _send_message(title, content):
    # Wechat message
    http_params = {'title': title, 'content': content}
    url = Msg_Base_Url + urlencode(http_params)
    res = request.urlopen(url)
    print(res.read().decode())


if __name__ == '__main__':
    try:
        cf = cp.ConfigParser()
        cf.read("../config.ini")
        JK_User = cf.get("Fetch", 'JK_User')
        JK_Token = cf.get("Fetch", 'JK_Token')
        All_Stocks_File = cf.get("Fetch", 'All_Stocks_File')
        Cursor_File = cf.get("Fetch", 'Cursor_File')
        Price_Dir = cf.get("Fetch", 'Price_Dir')
        Price_Start_Date = cf.get("Fetch", 'Price_Start_Date')
        Price_End_Date = cf.get("Fetch", 'Price_End_Date')
        Msg_Base_Url = cf.get("MSG", 'Msg_Base_Url')
    except:
        tb.print_exc()
    if not os.path.exists(Price_Dir):
        os.mkdir(Price_Dir)
    # _send_message("侧Title", "发达Content")
    # _check_spare()
    # get_all_stocks_info()
    get_stock_price()
