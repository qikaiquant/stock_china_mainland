import itertools
import random
import traceback

import matplotlib.pyplot as plt
import numpy
import talib

from strategy.base_strategy import *


class MacdWarmer(BaseWarmer):
    def warm(self):
        self.cache_tool.clear(int(self.cache_no))
        stocks = self.db_tool.get_stock_info(['stock_id'])
        cols = ['dt', 'close', 'open', 'money']
        for (stock_id,) in stocks:
            try:
                res = self.db_tool.get_price(stock_id, fields=cols, start_dt=self.warm_start_date,
                                             end_dt=self.warm_end_date)
                # 股票在2013-01-01前已退市
                if len(res) == 0:
                    continue
                res_df = pandas.DataFrame(res, columns=cols)
                res_df.set_index('dt', inplace=True)
                # 计算MACD指标
                res_df['dif'], res_df['dea'], res_df['hist'] = talib.MACD(numpy.array(res_df['close']), fastperiod=12,
                                                                          slowperiod=26, signalperiod=9)
                self.cache_tool.set(stock_id, res_df, self.cache_no, serialize=True)
            except Exception as e:
                traceback.print_exc()
                break


class MacdStrategy(BaseStrategy):
    def __init__(self, stg_id):
        super().__init__(stg_id)
        self.adhesion_period = conf_dict["STG"]["MACD"]["Adhesion_Period"]
        self.adhesion_cross_num = conf_dict["STG"]["MACD"]["Adhesion_Cross_Num"]

    @staticmethod
    def draw_survey(stock_id, price, pots, is_draw):
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

    def survey(self, stocks, is_draw):
        # 如果不显式传入股票代码，则随机选择30支股票做调研
        if (stocks is None) or (len(stocks) == 0):
            stocks = self.cache_tool.get(RAND_STOCK, COMMON_CACHE_ID, serialize=True)
            if not stocks:
                stocks = random.sample(self.all_stocks, 30)
                self.cache_tool.set(RAND_STOCK, stocks, COMMON_CACHE_ID, serialize=True)
        # 调研过程
        for stock_id in stocks:
            logging.info("+++++++++++++++++++" + stock_id + "++++++++++++++++++++")
            price = self.cache_tool.get(stock_id, self.cache_no, serialize=True)
            if price is None:
                continue
            status = 1  # 1:空仓，2：满仓
            trade_pots = []
            for dt in self.bt_tds:
                sig_action = Signal.KEEP.name
                [pre_dt] = get_preN_tds(self.all_trade_days, dt, 1)
                if (dt not in price.index) or (pre_dt not in price.index):
                    continue
                signal, reason = self.signal(stock_id, pre_dt, price)
                cur_price = price.loc[dt, 'open']
                # 寻找交易信号
                if (status == 1) and signal == Signal.BUY:
                    trade_pots.append((dt, cur_price, "B"))
                    sig_action = Signal.BUY.name
                    status = 2
                elif (status == 2) and signal == Signal.SELL:
                    trade_pots.append((dt, cur_price, "S"))
                    sig_action = Signal.SELL.name
                    status = 1
                logging.info("[%s][%s][%s]" % (str(dt), sig_action, reason))
            self.draw_survey(stock_id, price.loc[self._bt_sdt:self._bt_edt], trade_pots, is_draw)

    def signal(self, stock_id, dt, price):
        if (price is None) or (dt not in price.index):
            return Signal.KEEP, "Price Or Dt NULL"
        # check止盈止损
        cur_jiage = price.loc[dt, 'close']
        if self.stop_loss_surplus(stock_id, cur_jiage):
            return Signal.SELL, "StopLossSurplus"
        # macd信号
        d1, d0 = get_preN_tds(self.all_trade_days, dt, 2)
        if (d1 not in price.index) or (d0 not in price.index):
            return Signal.KEEP, "d0/d1 NULL"
        # 过去12天出现了超过3个x，说明黏着，不做交易
        cross_num = 0
        pre10_tds = get_preN_tds(self.all_trade_days, dt, self.adhesion_period)
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
        if cross_num >= self.adhesion_cross_num:
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

    def build_param_space(self):
        max_hold_space = range(1, 11, 1)
        stop_loss_space = list(range(8, 21, 2))
        stop_loss_space.append(-1)
        stop_surplus_space = list(range(15, 31, 2))
        stop_surplus_space.append(-1)
        adhesion_period_space = range(5, 31, 2)
        adhesion_cross_num_space = range(2, 10, 1)
        param_space = itertools.product(max_hold_space, stop_loss_space, stop_surplus_space, adhesion_period_space,
                                        adhesion_cross_num_space)
        return param_space

    def reset_param(self, param):
        if (param is None) or len(param) != 5:
            logging.error("Param is Unvalid, Pls Check.")
            return
        self.max_hold = param[0]
        self.stop_loss_point = param[1]
        self.stop_surplus_point = param[2]
        self.adhesion_period = param[3]
        self.adhesion_cross_num = param[4]
