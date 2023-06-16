import pandas
import talib

from utils.db_tool import *


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


if __name__ == '__main__':
    draw_stock_price('a', 'b', 'c')
