import logging

import matplotlib.pyplot as plt

from strategy.base_strategy import BaseStrategy

RAND_STOCK = 'RAND_STOCK'
CACHE_DB = 13


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
            init_money = 20000
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

    def backtest(self):
        self.survey()
