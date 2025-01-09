import abc
import os
import sys
import time
from abc import abstractmethod

import matplotlib.pyplot as plt
import mplfinance as mpf
import numpy
import pandas
import xlrd
from jqdatasdk import auth, logout, get_price, get_query_count, get_index_stocks, get_valuation, query, indicator, \
    get_fundamentals

sys.path.append(os.path.dirname(sys.path[0]))

from utils.db_tool import *
from utils.redis_tool import *
from utils.common import *


def draw_stock_price_trend(stock_id, sdate, edate):
    db_tool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                     conf_dict['Mysql']['Passwd'])
    prices = db_tool.get_price(stock_id, ['dt', 'close'], sdate, edate)
    df = pandas.DataFrame(prices, columns=['dt', 'close'])
    df.set_index('dt', inplace=True)
    index = [i for i in range(0, len(df['close']))]
    cofs = numpy.polyfit(index, df['close'], deg=1)
    ft_x = [df.index[0], df.index[len(df.index) - 1]]
    ft_y = [cofs[1], cofs[0] * (len(df['close']) - 1) + cofs[1]]
    print(cofs)

    plt.figure(figsize=(10, 6), dpi=100)
    plt.plot(df.index, df['close'], color='blue', label=stock_id)
    plt.plot(ft_x, ft_y, color='red', label="Fit")

    plt.legend()
    plt.show()


def test_mpf(stock_id, sdate, edate):
    cols = ['dt', 'open', 'high', 'low', 'close', 'volumn']
    db_tool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                     conf_dict['Mysql']['Passwd'])
    prices = db_tool.get_price(stock_id, cols, sdate, edate)
    df = pandas.DataFrame(prices, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
    df.index = pandas.DatetimeIndex(df['Date'])
    del df['Date']
    print(df.head(10))
    mpf.plot(df,
             type='line',
             ylabel="price",
             style='default',
             title='TTTTT',
             volume=True,
             mav=(5, 10),
             figratio=(5, 3),
             ylabel_lower="Volume")


def test_speed():
    # 初始化数据库
    dbtool = DBTool(conf_dict['Mysql']['host'], conf_dict['Mysql']['port'], conf_dict['Mysql']['user'],
                    conf_dict['Mysql']['passwd'])
    # 初始化Redis
    cachetool = RedisTool(conf_dict['Redis']['host'], conf_dict['Redis']['port'], conf_dict['Redis']['passwd'])
    stocks = ["600667.XSHG"]
    t0 = time.time()
    for stock_id in stocks:
        dbtool.get_price(stock_id, [])
    t1 = time.time()
    print("读数据库耗时:", t1 - t0)
    for stock_id in stocks:
        cachetool.get(stock_id, 0, serialize=True)
    t2 = time.time()
    print("读缓存耗时:", t2 - t1)


def test_config():
    print(conf_dict)


def test_redis_db_type():
    _conn = redis.Redis(host=conf_dict['Redis']['Host'], port=conf_dict['Redis']['Port'],
                        password=conf_dict['Redis']['Passwd'])


def _get_stock_info():
    # 初始化数据库
    dbtool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                    conf_dict['Mysql']['Passwd'])
    price = dbtool.get_stock_info(['start_date'])
    for (t,) in price:
        print(type(t))
        break


def test_redis_dump():
    redistool = RedisTool(conf_dict['Redis']['Host'], conf_dict['Redis']['Port'], conf_dict['Redis']['Passwd'])
    print(redistool.get("key", 0))


def _test_excel():
    f = xlrd.open_workbook("D:\\SwClassCode_2021.xls")
    table = f.sheets()[0]
    name = f.sheet_names()
    print(name)
    print(table.nrows)
    for i in table:
        if i[0].value == '行业代码':
            continue
        cid = int(i[0].value)
        indus_level_1 = i[1].value
        indus_level_2 = i[2].value
        indus_level_3 = i[3].value
        print(cid, indus_level_1, type(indus_level_2), type(indus_level_3))
    pass


def compare_price():
    db_tool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                     conf_dict['Mysql']['Passwd'])
    stocks = db_tool.get_stock_info(['stock_id'])
    stock_list = []
    for (stock_id,) in stocks:
        stock_list.append(stock_id)

    auth(conf_dict['DataSource']['JK_User'], conf_dict['DataSource']['JK_Token'])
    print(get_query_count())
    for stock_id in stock_list:
        print("+++++" + stock_id)
        res = db_tool.get_price(stock_id, ['dt', 'close', 'factor'])
        if len(res) == 0:
            print(stock_id + ' IS NULL')
            continue
        start_dt = res[0][0]
        end_dt = res[-1][0]
        pi = get_price(stock_id, end_date=end_dt, start_date=start_dt, frequency='daily',
                       fields=['close', 'factor'])
        for item in res:
            dt = item[0]
            db_close = item[1]
            db_facotr = item[2]
            jk_close = pi.loc[str(dt), 'close']
            jk_factor = pi.loc[str(dt), 'factor']
            if db_close != jk_close:
                print("DIFF!!!" + stock_id)
                print(dt, db_close, jk_close)
                print(dt, db_facotr, jk_factor)
                break
    logout()


class VirtualBase(abc.ABC):
    def __init__(self, a):
        self.a = a

    def output(self):
        print("HHHHH", self.a)

    @abstractmethod
    def virtualoutput(self):
        pass


class RealBase(VirtualBase):

    def virtualoutput(self):
        print("VVVVV", self.a)


class Father:
    def __init__(self, aa):
        print("Father:" + aa)


class Son(Father):
    def __init__(self, aa, bb):
        super().__init__(aa)
        print("Son:" + bb)


def mysql_float2double():
    db_tool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                     conf_dict['Mysql']['Passwd'])
    sql = "select TABLE_NAME from information_schema.TABLES where table_schema=\'quant_stock\'"
    res = db_tool.run_raw_sql(sql)
    tables = []
    for (atn) in res:
        name = atn[0]
        if name.startswith("price") or name.startswith("valuation"):
            tables.append(name)
    for t in tables:
        print(t)
        sql = ("select column_name, data_type from information_schema.columns where table_schema='quant_stock' "
               "and table_name=\'") + t + "\'"
        res = db_tool.run_raw_sql(sql)
        for (column, dtype) in res:
            if dtype == "float":
                modify_sql = 'alter table quant_stock.' + t + " modify " + column + ' double'
                db_tool.run_raw_sql(modify_sql, False)


def fetch_jq_data():
    db_tool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                     conf_dict['Mysql']['Passwd'])
    auth(conf_dict['DataSource']['JK_User'], conf_dict['DataSource']['JK_Token'])
    # q = query(indicator).filter(indicator.code == "002269.XSHE")
    # df = get_fundamentals(q, statDate="2024q2")
    df = get_valuation("000300.XSHG", start_date="2024-12-04", end_date="2024-12-04", fields=[])
    print(df)
    logout()


if __name__ == '__main__':
    fetch_jq_data()
