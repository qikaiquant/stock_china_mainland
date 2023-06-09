import talib
import pandas

from strategy.base_strategy import *
from utils.misc import *
import matplotlib.pyplot as plt


class MaStrategy(BaseStrategy):
    def draw(self):
        price = pandas.read_sql(
            " select dt,close,money from quant_stock.price_daily_r3  where sid = '000025.XSHE' limit 100",
            con=self.ctx.db_tool.get_conn(), index_col=['dt'])
        price['SMA15'] = talib.SMA(price['close'], timeperiod=15)
        price['SMA7'] = talib.SMA(price['close'], timeperiod=7)
        price['SMA15'].fillna(method='bfill', inplace=True)
        price['SMA7'].fillna(method='bfill', inplace=True)

        plt.figure(figsize=(10, 6), dpi=100)
        plt.plot(price.index, price['SMA15'], color='blue', label='SMA15')
        plt.plot(price.index, price['SMA7'], color='red', label='SMA7')
        plt.legend()
        plt.show()
        print(price.head(50))

    def backtest(self):
        stocks = self.ctx.db_tool.get_stock_info(['stock_id', 'cn_name', 'start_date', 'end_date'])
        count = 0
        for stock in stocks:
            sh_name = stock[1]
            res = self.ctx.db_tool.get_price(stock[0], ['dt', 'close', 'money'], stock[2], stock[3])
            stock_df = pandas.DataFrame(res, columns=['dt', 'close', 'money'])
            count += 1
            log(sh_name + " Loaded, Index " + str(count))
            break
