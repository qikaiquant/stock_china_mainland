import matplotlib.pyplot as plt

from strategy.base_strategy import *
from utils.common import *


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

    plt.show()
    # fn = "D:\\test\\survey\\" + stock_id + ".jpg"
    # plt.savefig(fn, dpi=600)


class MacdStrategy(BaseStrategy):

    def _signal(self, dt, price, ext_dict):
        if price is None:
            return Signal.KEEP
        if (ext_dict['day0'] not in price.index) or (ext_dict['day1'] not in price.index) or (dt not in price.index):
            return Signal.KEEP

        is_stinged = False
        # 过去12天出现了超过3个x，说明黏着，不做交易
        cross_num = 0
        pre10_tds = get_preN_tds(self.all_trade_days, dt, 16)
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
            is_stinged = True
            return Signal.SELL
        # 寻找交易信号，简单的金叉死叉
        day0_fast = price.loc[ext_dict['day0'], 'dif']
        day0_slow = price.loc[ext_dict['day0'], 'dea']
        day1_fast = price.loc[ext_dict['day1'], 'dif']
        day1_slow = price.loc[ext_dict['day1'], 'dea']

        sort_value = price.loc[ext_dict['day1'], 'money']
        if sort_value < 100000:
            return Signal.KEEP
        if (day0_slow > day0_fast) and (day1_slow < day1_fast) and (not is_stinged):
            return Signal.BUY
        if (day0_slow < day0_fast) and (day1_slow > day1_fast):
            return Signal.SELL
        return Signal.KEEP

    def _survey(self, stocks):
        # 如果不显式传入股票代码，则随机选择30支股票做调研
        if (stocks is None) or (len(stocks) == 0):
            stocks = self.cache_tool.get(RAND_STOCK, self.cache_no, serialize=True)
            if not stocks:
                stocks = []
                sql = 'select stock_id from quant_stock.stock_info where end_date > \'2013-01-01\' order by rand() ' \
                      'limit 30'
                res = self.db_tool.exec_raw_select(sql)
                for (sid,) in res:
                    stocks.append(sid)
                self.cache_tool.set(RAND_STOCK, stocks, COMMON_CACHE_ID, serialize=True)
                stocks = stocks
        # 回测周期调研
        for stock_id in stocks:
            all_price = self.cache_tool.get(stock_id, self.cache_no, serialize=True)
            price = all_price.loc[self.bt_sdt:self.bt_edt]
            status = 1  # 1:空仓，2：满仓
            pots = []
            for i in range(2, len(self.bt_tds) - 1):
                ext_dict = {'day0': self.bt_tds[i - 2], 'day1': self.bt_tds[i - 1]}
                signal = self._signal(self.bt_tds[i], all_price, ext_dict)
                cur_price = price.loc[self.bt_tds[i], 'avg']
                # 寻找交易信号
                if (status == 1) and signal == Signal.BUY:
                    pots.append((self.bt_tds[i], cur_price, "B"))
                    logging.info("Buy at Price [" + str(cur_price) + "] At Day [" + str(self.bt_tds[i]) + ']')
                    status = 2
                if (status == 2) and signal == Signal.SELL:
                    pots.append((self.bt_tds[i], cur_price, "S"))
                    logging.info("Sell at Price [" + str(cur_price) + "] At Day [" + str(self.bt_tds[i]) + ']')
                    status = 1
            _draw_survery(stock_id, price, pots)

    def _backtest(self):
        # 载入benchmark
        self.cache_tool.set(BENCHMARK_KEY, self.daily_benchmark, COMMON_CACHE_ID, serialize=True)
        # 遍历所有回测交易日
        for i in self.bt_tds:
            print(i)
            action_log = {'Buy': [], 'Sell': [], 'Hold': []}
            (day1, day0) = get_preN_tds(self.all_trade_days, i, 2)
            ext_dict = {'day0': day0, 'day1': day1}
            position = self.position
            # Check当前Hold是否需要卖出
            for stock_id in list(position.hold.keys()):
                price = self.cache_tool.get(stock_id, self.cache_no, serialize=True)
                if self._signal(i, price, ext_dict) == Signal.SELL:
                    position.sell(stock_id, price.loc[i, 'avg'], sell_all=True)
                    action_log['Sell'].append((stock_id, price.loc[i, 'avg']))
            # 不满仓，补足
            if position.can_buy():
                # 遍历所有股票，补足持仓
                candidate = []
                for stock in self.all_stocks:
                    price = self.cache_tool.get(stock, self.cache_no, serialize=True)
                    if self._signal(i, price, ext_dict) == Signal.BUY:
                        candidate.append((stock, price.loc[i, 'money'], price.loc[i, 'avg']))
                candidate.sort(key=lambda x: x[1], reverse=True)
                for can in candidate:
                    if position.buy(can[0], can[2]):
                        action_log['Buy'].append((can[0], can[2]))
                    if not position.can_buy():
                        break
            self.fill_daily_status(i, action_log)
        self.cache_tool.set(RES_KEY, self.daily_status, COMMON_CACHE_ID, serialize=True)

    def backtest(self):
        # self._backtest()
        self._survey(["300142.XSHE"])
