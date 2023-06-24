from enum import Enum

import matplotlib.pyplot as plt

from strategy.base_strategy import BaseStrategy
from utils.common import *

RAND_STOCK = 'RAND_STOCK'
NW_KEY = "NW_KEY"
All_Trade_Days = []
All_Stocks = []


class Signal(Enum):
    BUY = 0
    SELL = 1
    KEEP = 2


def _draw_survery(stock_id, price, pots):
    fig = plt.figure(figsize=(10, 6), dpi=100)
    plt.title(stock_id)

    ax1 = fig.add_subplot(211)
    ax1.plot(price.index, price['close'], color='black', label='Close Price')
    ax1.grid(linestyle='--')
    ax1.legend()

    ax2 = fig.add_subplot(212)
    ax2.plot(price.index, price['dif'], color='red', label='DIF')
    ax2.plot(price.index, price['dea'], color='blue', label='DEA')
    for (a, b, c) in pots:
        ax2.annotate(xy=(a, price.loc[a, 'dif']), text=c)
    ax2.grid(linestyle='--')
    ax2.legend()

    # plt.show()
    fn = "D:\\test\\survey\\" + stock_id + ".jpg"
    plt.savefig(fn, dpi=600)


class MacdStrategy(BaseStrategy):
    def __init__(self, ctx):
        super().__init__(ctx)

    @staticmethod
    def _signal(dt, price, ext_dict):
        if price is None:
            return Signal.KEEP
        if (ext_dict['day0'] not in price.index) or (ext_dict['day1'] not in price.index) or (dt not in price.index):
            return Signal.KEEP
        day0_fast = price.loc[ext_dict['day0'], 'dif']
        day0_slow = price.loc[ext_dict['day0'], 'dea']
        day1_fast = price.loc[ext_dict['day1'], 'dif']
        day1_slow = price.loc[ext_dict['day1'], 'dea']

        sort_value = price.loc[ext_dict['day1'], 'money']
        if sort_value < 100000:
            return Signal.KEEP
        # 寻找交易信号
        if (day0_slow > day0_fast) and (day1_slow < day1_fast):
            return Signal.BUY
        if (day0_slow < day0_fast) and (day1_slow > day1_fast):
            return Signal.SELL
        return Signal.KEEP

    def _survey(self):
        # 随机抽取30条股票做验证，暂存到cache里
        stocks = self.ctx.cache_tool.get(RAND_STOCK, self.ctx.cache_no, serialize=True)
        if not stocks:
            cache_item = []
            sql = 'select stock_id from quant_stock.stock_info where end_date > \'2013-01-01\' order by rand() limit 30'
            res = self.ctx.db_tool.exec_raw_select(sql)
            for (sid,) in res:
                cache_item.append(sid)
            self.ctx.cache_tool.set(RAND_STOCK, cache_item, self.ctx.cache_no, serialize=True)
            stocks = cache_item
        # 回测周期调研
        for stock_id in stocks:
            all_price = self.ctx.cache_tool.get(stock_id, self.ctx.cache_no, serialize=True)

            price = all_price.loc[self.ctx.bt_sdt:self.ctx.bt_edt]
            status = 1  # 1:空仓，2：满仓
            pots = []
            for i in range(2, len(self.ctx.bt_tds) - 1):
                ext_dict = {'day0': self.ctx.bt_tds[i - 2], 'day1': self.ctx.bt_tds[i - 1]}
                signal = MacdStrategy._signal(self.ctx.bt_tds[i], price, ext_dict)
                cur_price = price.loc[self.ctx.bt_tds[i], 'avg']
                # 寻找交易信号
                if (status == 1) and signal == Signal.BUY:
                    pots.append((self.ctx.bt_tds[i], cur_price, "B"))
                    logging.info("Buy at Price [" + str(cur_price) + "] At Day [" + str(self.ctx.bt_tds[i]) + ']')
                    status = 2
                if (status == 2) and signal == Signal.SELL:
                    pots.append((self.ctx.bt_tds[i], cur_price, "S"))
                    logging.info("Sell at Price [" + str(cur_price) + "] At Day [" + str(self.ctx.bt_tds[i]) + ']')
                    status = 1
            _draw_survery(stock_id, price, pots)

    def _backtest(self):
        # 遍历所有回测交易日
        for i in self.ctx.bt_tds:
            print(i)
            action_log = []
            (day1, day0) = get_preN_tds(All_Trade_Days, i, 2)
            ext_dict = {'day0': day0, 'day1': day1}
            position = self.ctx.position
            # Check当前Hold是否需要卖出
            for stock_id in list(position.hold.keys()):
                price = self.ctx.cache_tool.get(stock_id, self.ctx.cache_no, serialize=True)
                if MacdStrategy._signal(i, price, ext_dict) == Signal.SELL:
                    position.sell(stock_id, price.loc[i, 'avg'], sell_all=True)
                    action_log.append("-" + stock_id + "(" + str(price.loc[i, 'avg']) + ")")
            # 不满仓，补足
            if position.can_buy():
                # 遍历所有股票，补足持仓
                candidate = []
                for stock in All_Stocks:
                    price = self.ctx.cache_tool.get(stock, self.ctx.cache_no, serialize=True)
                    if MacdStrategy._signal(i, price, ext_dict) == Signal.BUY:
                        candidate.append((stock, price.loc[i, 'money'], price.loc[i, 'avg']))
                candidate.sort(key=lambda x: x[1], reverse=True)
                for can in candidate:
                    if position.buy(can[0], can[2]):
                        action_log.append("+" + can[0] + "(" + str(can[2]) + ")")
                    if not position.can_buy():
                        break
            self.ctx.fill_detail(i, action_log)
        self.ctx.cache_tool.set(NW_KEY, self.ctx.daily_nw, self.ctx.cache_no, serialize=True)

    def backtest(self):
        # load所有股票、交易日信息
        res = self.ctx.db_tool.get_stock_info(['stock_id'])
        for (sid,) in res:
            All_Stocks.append(sid)
        res = self.ctx.db_tool.get_trade_days()
        for (td,) in res:
            All_Trade_Days.append(td)
        self._backtest()
        # self._survey()
