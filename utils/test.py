import os
import random
import sys
import time

import matplotlib.pyplot as plt
import pandas
import talib

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
    db_tool = DBTool(conf_dict['Mysql']['host'], conf_dict['Mysql']['port'], conf_dict['Mysql']['user'],
                     conf_dict['Mysql']['passwd'])
    prices = db_tool.get_price(stock_id, ['dt', 'close'], sdate, edate)
    df = pandas.DataFrame(prices, columns=['dt', 'close'])
    df['SMA15'] = talib.SMA(df['close'], timeperiod=15)
    df['SMA7'] = talib.SMA(df['close'], timeperiod=7)
    df['SMA15'].fillna(method='bfill', inplace=True)
    df['SMA7'].fillna(method='bfill', inplace=True)

    plt.figure(figsize=(10, 6), dpi=100)
    plt.plot(df['dt'], df['close'], color='black', label=stock_id)
    plt.plot(df['dt'], df['SMA7'], color='red', label="SMA7")
    plt.plot(df['dt'], df['SMA15'], color='blue', label="SMA15")
    plt.legend()
    plt.show()


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
        cachetool.get(stock_id, db.DB_PRICE, serialize=True)
    t2 = time.time()
    print("读缓存耗时:", t2 - t1)


def test_config():
    print(conf_dict)


def add_sma():
    pass


if __name__ == '__main__':
    add_sma()
