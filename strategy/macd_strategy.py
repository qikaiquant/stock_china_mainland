import itertools
import random
import time

import matplotlib.pyplot as plt

from strategy.base_strategy import *


def _macd_draw_survey(stock_id, price, pots, is_draw):
    fig, ax1 = plt.subplots(figsize=(10, 6), dpi=100)
    plt.title(stock_id)
    ax2 = ax1.twinx()

    ax1.plot(price.index, price['close'], color='black', label='Open Price')
    for (dt, BuyOrSell) in pots:
        ax1.annotate(xy=(dt, price.loc[dt, 'close']), text=BuyOrSell)
    ax1.grid(linestyle='--')
    ax1.legend(loc=1)

    ax2.plot(price.index, price['dif'], color='red', label='DIF-Fast')
    ax2.plot(price.index, price['dea'], color='blue', label='DEA-Slow')
    ax2.grid(linestyle='--')
    ax2.legend(loc=2)
    print(pots)
    if is_draw:
        plt.show()
    else:
        fn = "D:\\test\\" + stock_id + ".jpg"
        plt.savefig(fn, dpi=600)


class MacdStrategy(BaseStrategy):
    def __init__(self, stg_id):
        super().__init__(stg_id)
        self.adhesion_period = conf_dict["STG"]["MACD"]["Adhesion_Period"]
        self.adhesion_cross_num = conf_dict["STG"]["MACD"]["Adhesion_Cross_Num"]

    def signal(self, stock_id, cur_dt):
        [pre_dt] = get_preN_tds(self.all_trade_days, cur_dt, days=1)
        price = self.cache_tool.get(stock_id, self.cache_no, serialize=True)
        if (price is None) or (pre_dt not in price.index):
            return [Signal.KEEP, None, "Price Or Dt NULL", None]
        # check止盈止损
        cur_jiage = price.loc[pre_dt, 'close']
        if self.stop_loss_surplus(stock_id, cur_jiage):
            return [Signal.SELL, None, "StopLossSurplus", None]
        # macd信号
        d1, d0 = get_preN_tds(self.all_trade_days, cur_dt, 2)
        if (d1 not in price.index) or (d0 not in price.index):
            return [Signal.KEEP, None, "d0/d1 NULL", None]
        # 过去A天出现了超过B个x，说明黏着，不做交易
        cross_num = 0
        pre10_tds = get_preN_tds(self.all_trade_days, cur_dt, self.adhesion_period)
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
            return [Signal.KEEP, None, "Too Many Crossed", None]
        # 寻找交易信号，简单的金叉死叉
        day0_fast = price.loc[d0, 'dif']
        day0_slow = price.loc[d0, 'dea']
        day1_fast = price.loc[d1, 'dif']
        day1_slow = price.loc[d1, 'dea']

        if (day0_slow > day0_fast) and (day1_slow < day1_fast):
            return [Signal.BUY, None, "Corss", price.loc[pre_dt, 'money']]
        if (day0_slow < day0_fast) and (day1_slow > day1_fast):
            return [Signal.SELL, None, "Cross", None]
        return [Signal.KEEP, None, "Nothing", None]

    def pick_candidate(self, dt):
        candidates = []
        for stockid in self.all_stocks:
            [sig, jiage, _, rank_score] = self.signal(stockid, dt)
            if sig == Signal.BUY:
                candidates.append((stockid, jiage, rank_score))
        candidates.sort(key=lambda x: x[2], reverse=False)
        return candidates

    def adjust_position(self, dt):
        position = self.position
        trader = self.position.trader
        candidates = None
        # 初始化，所有槽位都设置为INIT
        for slot in position.hold:
            slot[3] = PositionStatus.INIT
        # 是个很复杂的状态机，见笔记
        while True:
            for i in range(0, len(position.hold)):
                slot = position.hold[i]
                stock_id = slot[0]
                match slot[3]:
                    case PositionStatus.INIT:
                        if stock_id is None:
                            slot[3] = PositionStatus.EMPTY
                        else:
                            slot[3] = PositionStatus.NEED_CHECK
                    case PositionStatus.NEED_CHECK:
                        [sig, jiage, _, _] = self.signal(stock_id, dt)
                        if sig == Signal.SELL:
                            slot[3] = PositionStatus.WAIT_SELL
                            trader.sell(position, slot, dt, jiage)
                        else:
                            slot[3] = PositionStatus.KEEP
                    case PositionStatus.EMPTY:
                        # 算一遍候选集合，注意只算一遍
                        if candidates is None:
                            candidates = self.pick_candidate(dt)
                        if len(candidates) == 0:
                            slot[3] = PositionStatus.KEEP
                        else:
                            slot[3] = PositionStatus.WAIT_BUY
                            [stock_id, jiage, _] = candidates.pop()
                            trader.buy(position, slot, dt, stock_id, jiage)
                    case PositionStatus.BUY_FAIL:
                        # 买入失败，置空重新开始
                        slot[3] = PositionStatus.EMPTY
            keep_count = 0
            for slot in position.hold:
                if slot[3] == PositionStatus.KEEP:
                    keep_count += 1
            if keep_count == len(position.hold):
                break
            logging.info(str(position.hold))
            time.sleep(1)

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
            logging.error("Param is Invalid, Pls Check.")
            raise Exception("Param is Invalid")
        self.max_hold = param[0]
        self.stop_loss_point = param[1]
        self.stop_surplus_point = param[2]
        self.adhesion_period = param[3]
        self.adhesion_cross_num = param[4]

    def survey(self):
        # 选30支股票做调研
        stocks = self.cache_tool.get(RAND_STOCK, COMMON_CACHE_ID, serialize=True)
        if (stocks is None) or (len(stocks) == 0):
            if not stocks:
                stocks = random.sample(self.all_stocks, 3)
                self.cache_tool.set(RAND_STOCK, stocks, COMMON_CACHE_ID, serialize=True)
        # 调研过程
        for stock_id in stocks:
            logging.info("+++++++++++++++++++" + stock_id + "++++++++++++++++++++")
            price = self.cache_tool.get(stock_id, self.cache_no, serialize=True)
            if price is None:
                continue
            trade_pots = []
            start_date = datetime.strptime('2024-09-01', '%Y-%m-%d').date()
            end_date = datetime.strptime('2024-11-30', '%Y-%m-%d').date()
            survey_days = get_trade_days(self.all_trade_days, start_date, end_date)
            for dt in survey_days:
                [pre_dt] = get_preN_tds(self.all_trade_days, dt, 1)
                if (dt not in price.index) or (pre_dt not in price.index):
                    continue
                [sig, _, info, _] = self.signal(stock_id, dt)
                # 寻找交易信号
                if sig == Signal.BUY:
                    trade_pots.append((dt, "B"))
                elif sig == Signal.SELL:
                    trade_pots.append((dt, "S"))
                logging.info("[%s][%s][%s]" % (str(dt), sig.name, info))
            _macd_draw_survey(stock_id, price, trade_pots, False)
