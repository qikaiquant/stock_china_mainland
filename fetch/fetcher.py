import datetime as dt
import time
import traceback as tb
import pandas as pd
from jqdatasdk import *


def get_all_stocks_info():
    fp = None
    try:
        auth("17896008784", "WJnyjdc1983")
        fp = open("all_stocks_info.txt", 'w')
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
        fp = open(code.replace(".", "_") + ".txt", 'w')
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
        line = fp_cursor.readline()
        while line:
            code = line.strip().split(' ')[1]
            finished_set.add(code)
            line = fp_cursor.readline()
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
    end_date = dt.datetime.strptime('2023-05-06 18:00:00', formatstr)
    start_date = dt.datetime.strptime('2017-01-01 00:00:00', formatstr)

    cursor_file = 'cursor.txt'
    finished_set = _load_finished(cursor_file)

    try:
        fp = open("all_stocks_info.txt", 'r')
        fp_cursor = open(cursor_file, 'a+')
        auth("17896008784", "WJnyjdc1983")
        line = fp.readline()
        while line:
            infos = line.strip().split(',')
            code = infos[0]
            if code in finished_set:
                line = fp.readline()
                continue
            ipo_date = dt.datetime.strptime(infos[3], formatstr)
            if start_date < ipo_date:
                start_date = ipo_date
            pi = get_price(code, end_date=end_date, start_date=start_date, frequency='daily',
                           fields=['open', 'close', 'low', 'high', 'volume', 'money', 'factor', 'high_limit',
                                   'low_limit', 'avg', 'pre_close', 'paused'])
            if _output_stock_price(code, pd.DataFrame(pi)):
                print("Code " + code + " Finished.", file=fp_cursor)
            if get_query_count()['spare'] < 3000:
                print("Daily Limited Reached.")
                break
            else:
                line = fp.readline()
                time.sleep(1)
    except:
        tb.print_exc()
    finally:
        if fp is not None:
            fp.close()
        if fp_cursor is not None:
            fp_cursor.close()
        logout()


def _check_spare():
    auth("17896008784", "WJnyjdc1983")
    print(str(get_query_count()))
    logout()


if __name__ == '__main__':
    # get_all_stocks_info()
    # get_stock_price()
    _check_spare()
