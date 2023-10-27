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
    def __init__(self, sdt, edt, dbt, ct, cno, total_budget, max_hold):
        super().__init__(sdt, edt, dbt, ct, cno, total_budget, max_hold)
        if "StopLossPoint" in conf_dict['STG']['KDJ']:
            self.stop_loss_point = conf_dict['STG']['KDJ']['StopLossPoint']
        if "StopSurplusPoint" in conf_dict['STG']['KDJ']:
            self.stop_surplus_point = conf_dict['STG']['KDJ']['StopSurplusPoint']

    def _signal(self, stock_id, dt, price):
        # check止盈止损
        if (price is None) or (dt not in price.index):
            return Signal.KEEP
        cur_jiage = price.loc[dt, 'close']
        if self.stop_loss_surplus(stock_id, cur_jiage):
            return Signal.SELL
        d1, d0 = get_preN_tds(self.all_trade_days, dt, 2)
        if (d1 not in price.index) or (d0 not in price.index):
            return Signal.KEEP
        # 超卖区金叉
        if (price.loc[d0, 'D'] > price.loc[d0, 'K']) and (price.loc[d1, "D"] < price.loc[d1, "K"]):
            cross_point = (price.loc[d0, 'D'] + price.loc[d1, 'D']) / 2
            if cross_point < 20:
                return Signal.BUY
        # 超买区死叉
        if (price.loc[d0, 'D'] < price.loc[d0, 'K']) and (price.loc[d1, "D"] > price.loc[d1, "K"]):
            cross_point = (price.loc[d0, 'D'] + price.loc[d1, 'D']) / 2
            if cross_point > 80:
                return Signal.SELL
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
            _draw_survery(stock_id, price.loc[self._bt_sdt:self._bt_edt], trade_pots)

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
            self._fill_daily_status(i, action_log)
        self.cache_tool.set(RES_KEY, self.daily_status, COMMON_CACHE_ID, serialize=True)

    def backtest(self):
        self._backtest()
