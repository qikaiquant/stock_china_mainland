import random
import matplotlib.pyplot as plt

from strategy.base_strategy import *

"""
策略框架。
新建策略，需要实现_singal()函数和warmer中的数据预处理函数ph_XXXX
理论上，实现了_singal()函数和warmer中的ph_XXX，并在config.json中配置好，
就可以做策略的验证。
"""


def _draw_survery(stock_id, price, pots):
    fig = plt.figure(figsize=(10, 6), dpi=100)
    plt.title(stock_id)

    ax1 = fig.add_subplot(211)
    ax1.plot(price.index, price['close'], color='black', label='Close Price')
    ax1.grid(linestyle='--')
    ax1.legend()

    ax2 = fig.add_subplot(212)
    # ax2.plot(price.index, price['K'], color='red', label='K')
    for (a, b, c) in pots:
        ax2.annotate(xy=(a, price.loc[a, 'D']), text=c)
    ax2.grid(linestyle='--')
    ax2.legend()

    plt.show()
    # fn = "D:\\test\\survey\\" + stock_id + ".jpg"
    # plt.savefig(fn, dpi=600)


class XXXStrategy(BaseStrategy):
    def __init__(self, sdt, edt, dbt, ct, cno, total_budget, max_hold):
        super().__init__(sdt, edt, dbt, ct, cno, total_budget, max_hold)
        if "StopLossPoint" in conf_dict['STG']['XXX']:
            self.stop_loss_point = conf_dict['STG']['XXX']['StopLossPoint']
        if "StopSurplusPoint" in conf_dict['STG']['XXX']:
            self.stop_surplus_point = conf_dict['STG']['XXX']['StopSurplusPoint']

    def _signal(self, stock_id, dt, price):
        return Signal.KEEP

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
                if dt not in price.index:
                    continue
                signal = self._signal(stock_id, dt, price)
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

    def _backtest(self):
        # 载入benchmark
        self.cache_tool.set(BENCHMARK_KEY, self.daily_benchmark, COMMON_CACHE_ID, serialize=True)
        # 遍历所有回测交易日
        for i in self.bt_tds:
            print(i)
            action_log = {'Buy': [], 'Sell': [], 'Hold': []}
            position = self.position
            # Check当前Hold是否需要卖出
            for stock_id in list(position.hold.keys()):
                price = self.cache_tool.get(stock_id, self.cache_no, serialize=True)
                if self._signal(stock_id, i, price) == Signal.SELL:
                    position.sell(stock_id, price.loc[i, 'avg'], sell_all=True)
                    action_log['Sell'].append((stock_id, price.loc[i, 'avg']))
            # 不满仓，补足
            if position.can_buy():
                # 遍历所有股票，补足持仓
                candidate = []
                for stock in self.all_stocks:
                    price = self.cache_tool.get(stock, self.cache_no, serialize=True)
                    if self._signal(stock, i, price) == Signal.BUY:
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
        self._survey([])
