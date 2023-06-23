from enum import Enum

import matplotlib.pyplot as plt

from strategy.base_strategy import BaseStrategy
from utils.common import *

RAND_STOCK = 'RAND_STOCK'
CACHE_DB = 13
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
    fn = "D:\\test\\" + stock_id + ".jpg"
    plt.savefig(fn, dpi=600)


class MacdStrategy(BaseStrategy):
    def __init__(self, ctx):
        super().__init__(ctx)

    def survey(self):
        # 随机抽取30条股票做验证，暂存到cache里
        stocks = self.ctx.cache_tool.get(RAND_STOCK, CACHE_DB, serialize=True)
        if not stocks:
            cache_item = []
            sql = 'select stock_id from quant_stock.stock_info where end_date > \'2013-01-01\' order by rand() limit 30'
            res = self.ctx.db_tool.exec_raw_select(sql)
            for (sid,) in res:
                cache_item.append(sid)
            self.ctx.cache_tool.set(RAND_STOCK, cache_item, CACHE_DB, serialize=True)
            stocks = cache_item
        # 回测周期调研
        for stock_id in stocks:
            all_price = self.ctx.cache_tool.get(stock_id, 0, serialize=True)
            price = all_price.loc[self.ctx.bt_sdt:self.ctx.bt_edt]
            status = 1  # 1:空仓，2：满仓
            pots = []
            for i in range(2, len(self.ctx.bt_tds) - 1):
                if self.ctx.bt_tds[i - 2] not in price.index:
                    continue
                cur_price = price.loc[self.ctx.bt_tds[i], 'avg']
                day0_fast = price.loc[self.ctx.bt_tds[i - 2], 'dif']
                day0_slow = price.loc[self.ctx.bt_tds[i - 2], 'dea']
                day1_fast = price.loc[self.ctx.bt_tds[i - 1], 'dif']
                day1_slow = price.loc[self.ctx.bt_tds[i - 1], 'dea']
                # 寻找交易信号
                if (status == 1) and (day0_slow > day0_fast) and (day1_slow < day1_fast):
                    pots.append((self.ctx.bt_tds[i], cur_price, "B"))
                    logging.info("Buy at Price [" + str(cur_price) + "] At Day [" + str(self.ctx.bt_tds[i]) + ']')
                    status = 2
                if (status == 2) and (day0_slow < day0_fast) and (day1_slow > day1_fast):
                    pots.append((self.ctx.bt_tds[i], cur_price, "S"))
                    logging.info("Sell at Price [" + str(cur_price) + "] At Day [" + str(self.ctx.bt_tds[i]) + ']')
                    status = 1
            _draw_survery(stock_id, price, pots)

    def _signal(self, dt, stock_id, ext_dict):
        price = self.ctx.cache_tool.get(stock_id, self.ctx.cache_no, serialize=True)
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
            ext_dict['sv'] = sort_value
            ext_dict['jiage'] = price.loc[dt, 'avg']
            return Signal.BUY
        if (day0_slow < day0_fast) and (day1_slow > day1_fast):
            ext_dict['jiage'] = price.loc[dt, 'avg']
            return Signal.SELL
        return Signal.KEEP

    def backtest(self):
        # load所有股票、交易日信息
        res = self.ctx.db_tool.get_stock_info(['stock_id'])
        for (sid,) in res:
            All_Stocks.append(sid)
        res = self.ctx.db_tool.get_trade_days()
        for (td,) in res:
            All_Trade_Days.append(td)
        # 遍历所有回测交易日
        for i in self.ctx.bt_tds:
            (day1, day0) = get_preN_tds(All_Trade_Days, i, 2)
            ext_dict = {'day0': day0, 'day1': day1}
            hold_dict = self.ctx.position.hold
            # 验证Hold是否需要卖出
            for stock_id in list(hold_dict.keys()):
                if self._signal(i, stock_id, ext_dict) == Signal.SELL:
                    self.ctx.position.sell(stock_id, ext_dict['jiage'], sell_all=True)
            # 满仓，直接退出
            if len(hold_dict) >= 5:
                continue
            # 遍历所有股票，补足持仓
            candidate = []
            for stock in All_Stocks:
                if self._signal(i, stock, ext_dict) == Signal.BUY:
                    candidate.append((stock, ext_dict['sv'], ext_dict['jiage']))
            candidate.sort(key=lambda x: x[1], reverse=True)
            for can in candidate:
                self.ctx.position.buy(can[0], can[2], 20000)
                if len(hold_dict) == 5:
                    break
            print(i)
