import logging

import matplotlib.pyplot as plt
from strategy.base_strategy import *


class MacdStrategy(BaseStrategy):
    def draw_survey(self, stock_id, price, pots, is_draw):
        fig, ax1 = plt.subplots(figsize=(10, 6), dpi=100)
        plt.title(stock_id)
        ax2 = ax1.twinx()

        ax1.plot(price.index, price['open'], color='black', label='Open Price')
        for (dt, jiage, BuyOrSell) in pots:
            ax1.annotate(xy=(dt, price.loc[dt, 'open']), text=BuyOrSell)
        ax1.grid(linestyle='--')
        ax1.legend(loc=1)

        ax2.plot(price.index, price['dif'], color='red', label='DIF-Fast')
        ax2.plot(price.index, price['dea'], color='blue', label='DEA-Slow')
        ax2.grid(linestyle='--')
        ax2.legend(loc=2)

        if is_draw:
            plt.show()
        else:
            fn = "D:\\test\\" + stock_id + ".jpg"
            plt.savefig(fn, dpi=600)

    def signal(self, stock_id, dt, price):
        # check止盈止损
        if (price is None) or (dt not in price.index):
            return Signal.KEEP, "Price Or Dt NULL"
        cur_jiage = price.loc[dt, 'close']
        if self.stop_loss_surplus(stock_id, cur_jiage):
            return Signal.SELL, "StopLossSurplus"
        # macd信号
        d1, d0 = get_preN_tds(self.all_trade_days, dt, 2)
        if (d1 not in price.index) or (d0 not in price.index):
            return Signal.KEEP, "d0/d1 NULL"
        # 过去12天出现了超过3个x，说明黏着，不做交易
        cross_num = 0
        pre10_tds = get_preN_tds(self.all_trade_days, dt, 12)
        for i in range(1, len(pre10_tds)):
            if pre10_tds[i - 1] not in price.index or pre10_tds[i] not in price.index:
                continue
            day0_f = price.loc[pre10_tds[i - 1], 'dif']
            day0_s = price.loc[pre10_tds[i - 1], 'dea']
            dif1 = day0_f - day0_s
            day1_f = price.loc[pre10_tds[i], 'dif']
            day1_s = price.loc[pre10_tds[i], 'dea']
            dif2 = day1_f - day1_s
            if dif1 * dif2 < 0:
                cross_num += 1
        if cross_num >= 3:
            return Signal.KEEP, "Too Many Crossed"
        # 寻找交易信号，简单的金叉死叉
        day0_fast = price.loc[d0, 'dif']
        day0_slow = price.loc[d0, 'dea']
        day1_fast = price.loc[d1, 'dif']
        day1_slow = price.loc[d1, 'dea']

        if (day0_slow > day0_fast) and (day1_slow < day1_fast):
            return Signal.BUY, "Cross"
        if (day0_slow < day0_fast) and (day1_slow > day1_fast):
            return Signal.SELL, "Cross"
        return Signal.KEEP, "Nothing"

    def run(self):
        # self.survey(["002112.XSHE"], False)
        self.survey([], False)
    # self.backtest()
