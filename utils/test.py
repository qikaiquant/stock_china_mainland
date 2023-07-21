import os
import random
import sys
import time

import matplotlib.pyplot as plt
import mplfinance as mpf
import numpy
import pandas

sys.path.append(os.path.dirname(sys.path[0]))

from utils.db_tool import *
from utils.redis_tool import *
from utils.common import *


def draw():
    x = range(0, 120)
    y1 = [random.randint(25, 30) for i in x]
    y2 = [random.randint(11, 14) for i in x]

    fig = plt.figure(figsize=(10, 6), dpi=100)
    plt.rc('font', family='FangSong', size=14)
    # 左侧折线图
    left, bottom, width, height = 0.05, 0.2, 0.7, 0.6
    ax1 = fig.add_axes([left, bottom, width, height])
    ax1.plot(x, y1, color='darkred', label="Zhang")
    ax1.plot(x, y2, color='slategrey', label="Wang")
    ax1.grid(linestyle='--')
    ax1.set_facecolor('whitesmoke')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    # plt.title("000023.SZ")
    ax1.legend(loc='best')
    # 右侧数据指标表格
    left, bottom, width, height = 0.76, 0.2, 0.2, 0.6
    ax2 = fig.add_axes([left, bottom, width, height])
    celltext = [['收益', '120%', '250%'], ['年化收益', '120%', '250%'], ['夏普指数', '0.5', '2'],
                ['最大回撤', '50%', '20%']]
    columns = ['指标', "MA50", "基线"]
    ax2.axis('off')
    tb = ax2.table(cellText=celltext, colLabels=columns, loc='lower left', cellLoc='center', rowLoc='bottom')
    tb.scale(1.1, 1.3)

    # plt.savefig("/home/qikai/aaa.jpg", dpi=600)
    plt.show()


def draw_stock_price(stock_id, sdate, edate):
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
    _conn.select(12)
    _conn.set("name", 'zhangsan')
    print(_conn.get('name'))


def _get_stock_info():
    # 初始化数据库
    dbtool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                    conf_dict['Mysql']['Passwd'])
    price = dbtool.get_stock_info(['start_date'])
    for (t,) in price:
        print(type(t))
        break


if __name__ == '__main__':
    # time_str = datetime.now().strftime("%Y%m%d_%H_%M_%S")
    draw_stock_price("300142.XSHE", '2018-01-01', '2022-08-01')
    # test_mpf("300142.XSHE", '2022-01-01', '2023-07-01')
