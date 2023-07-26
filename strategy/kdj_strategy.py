import random
import matplotlib.pyplot as plt

from strategy.base_strategy import *


def _draw_survery(stock_id, price, pots):
    fig = plt.figure(figsize=(10, 6), dpi=100)
    plt.title(stock_id)

    ax1 = fig.add_subplot(211)
    ax1.plot(price.index, price['close'], color='black', label='Close Price')
    ax1.grid(linestyle='--')
    ax1.legend()

    ax2 = fig.add_subplot(212)
    ax2.plot(price.index, price['K'], color='red', label='K')
    ax2.plot(price.index, price['D'], color='blue', label='D')
    for (a, b, c) in pots:
        ax2.annotate(xy=(a, price.loc[a, 'D']), text=c)
    ax2.grid(linestyle='--')
    ax2.legend()

    plt.show()
    # fn = "D:\\test\\survey\\" + stock_id + ".jpg"
    # plt.savefig(fn, dpi=600)


class KdjStrategy(BaseStrategy):
    def _signal(self, dt, price):
        d1, d0 = get_preN_tds(self.all_trade_days, dt, 2)
        ret = Signal.KEEP
        # 超卖区金叉
        if (price.loc[d0, 'D'] > price.loc[d0, 'K']) and (price.loc[d1, "D"] < price.loc[d1, "K"]):
            cross_point = (price.loc[d0, 'D'] + price.loc[d1, 'D']) / 2
            print("Gold Cross " + str(cross_point))
            if cross_point < 20:
                ret = Signal.BUY
        # 超买区死叉
        if (price.loc[d0, 'D'] < price.loc[d0, 'K']) and (price.loc[d1, "D"] > price.loc[d1, "K"]):
            cross_point = (price.loc[d0, 'D'] + price.loc[d1, 'D']) / 2
            print("Dead Cross " + str(cross_point))
            if cross_point > 80:
                ret = Signal.SELL
        return ret

    def _survey(self, stocks):
        # 如果不显式传入股票代码，则随机选择30支股票做调研
        if (stocks is None) or (len(stocks) == 0):
            stocks = self.cache_tool.get(RAND_STOCK, COMMON_CACHE_ID, serialize=True)
            if not stocks:
                stocks = random.sample(self.all_stocks, 30)
                self.cache_tool.set(RAND_STOCK, stocks, COMMON_CACHE_ID, serialize=True)
        # 调研过程
        for stock_id in stocks:
            price = self.cache_tool.get(stock_id, self.cache_no, serialize=True)
            status = 1  # 1:空仓，2：满仓
            trade_pots = []
            for dt in self.bt_tds:
                signal = self._signal(dt, price)
                cur_price = price.loc[dt, 'avg']
                # 寻找交易信号
                if (status == 1) and signal == Signal.BUY:
                    trade_pots.append((dt, cur_price, "B"))
                    logging.info("Buy at Price [" + str(cur_price) + "] At Day [" + str(dt) + ']')
                    status = 2
                if (status == 2) and signal == Signal.SELL:
                    trade_pots.append((dt, cur_price, "S"))
                    logging.info("Sell at Price [" + str(cur_price) + "] At Day [" + str(dt) + ']')
                    status = 1
            _draw_survery(stock_id, price.loc[self.bt_sdt:self.bt_edt], trade_pots)

    def backtest(self):
        self._survey(["002537.XSHE"])
