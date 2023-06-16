import time

import pandas
import talib

from utils.db_tool import *
from utils.redis_tool import *


def draw_stock_price(stock_id, sdate, edate):
    conf_dict = load_config("../config/config.ini")
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
    conf_dict = load_config("../config/config.ini")
    # 初始化数据库
    dbtool = DBTool(conf_dict['Mysql']['host'], conf_dict['Mysql']['port'], conf_dict['Mysql']['user'],
                    conf_dict['Mysql']['passwd'])
    # 初始化Redis
    cachetool = RedisTool(conf_dict['Redis']['host'], conf_dict['Redis']['port'], conf_dict['Redis']['passwd'])
    stocks = ["600667.XSHG", "002886.XSHE", "688596.XSHG", "300374.XSHE", "301326.XSHE", "600723.XSHG", "603408.XSHG",
              "002615.XSHE", "600141.XSHG", "600596.XSHG", "300241.XSHE", "301057.XSHE", "300277.XSHE"]
    t0 = time.time()
    for stock_id in stocks:
        dbtool.get_price(stock_id, [])
    t1 = time.time()
    print("读数据库耗时:", t1 - t0)
    for stock_id in stocks:
        cachetool.get(stock_id, 0, serialize=True)
    t2 = time.time()
    print("读缓存耗时:", t2 - t1)


if __name__ == '__main__':
    test_speed()
