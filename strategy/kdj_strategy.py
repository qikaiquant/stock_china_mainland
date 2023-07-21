import random
import matplotlib.pyplot as plt

from strategy.base_strategy import *


def _draw_survery(stock_id, price):
    fig = plt.figure(figsize=(10, 6), dpi=100)
    plt.title(stock_id)

    ax1 = fig.add_subplot(311)
    ax1.plot(price.index, price['close'], color='black', label='Close Price')
    ax1.grid(linestyle='--')
    ax1.legend()

    ax2 = fig.add_subplot(312)
    ax2.plot(price.index, price['K'], color='red', label='K')
    ax2.plot(price.index, price['D'], color='blue', label='D')
    ax2.grid(linestyle='--')
    ax2.legend()

    ax3 = fig.add_subplot(313)
    ax3.bar(price.index, price['volumn'])
    ax3.grid(linestyle='--')
    ax3.legend()

    # plt.show()
    fn = "D:\\test\\survey\\" + stock_id + ".jpg"
    plt.savefig(fn, dpi=600)


class KdjStrategy(BaseStrategy):

    def _survey(self, stocks):
        # 如果不显式传入股票代码，则随机选择30支股票做调研
        if (stocks is None) or (len(stocks) == 0):
            stocks = self.cache_tool.get(RAND_STOCK, COMMON_CACHE_ID, serialize=True)
            if not stocks:
                stocks = random.sample(self.all_stocks, 30)
                self.cache_tool.set(RAND_STOCK, stocks, COMMON_CACHE_ID, serialize=True)
        # 调研过程
        for stock_id in stocks:
            all_price = self.cache_tool.get(stock_id, self.cache_no, serialize=True)
            price = all_price.loc[self.bt_sdt:self.bt_edt]
            _draw_survery(stock_id, price)

    def backtest(self):
        self._survey([])
