from strategy.base_strategy import *


class MacdStrategy(BaseStrategy):
    def __init__(self, sdt, edt, dbt, ct, cno, total_budget, max_hold):
        super().__init__(sdt, edt, dbt, ct, cno, total_budget, max_hold)
        if "StopLossPoint" in conf_dict['STG']['MACD']:
            self.stop_loss_point = conf_dict['STG']['MACD']['StopLossPoint']
        if "StopSurplusPoint" in conf_dict['STG']['MACD']:
            self.stop_surplus_point = conf_dict['STG']['MACD']['StopSurplusPoint']

    def _signal(self, stock_id, dt, price):
        # check止盈止损
        if (price is None) or (dt not in price.index):
            return Signal.KEEP
        cur_jiage = price.loc[dt, 'close']
        if self.stop_loss_surplus(stock_id, cur_jiage):
            return Signal.SELL
        # macd信号
        d1, d0 = get_preN_tds(self.all_trade_days, dt, 2)
        if (d1 not in price.index) or (d0 not in price.index):
            return Signal.KEEP
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
            return Signal.KEEP
        # 寻找交易信号，简单的金叉死叉
        day0_fast = price.loc[d0, 'dif']
        day0_slow = price.loc[d0, 'dea']
        day1_fast = price.loc[d1, 'dif']
        day1_slow = price.loc[d1, 'dea']

        if (day0_slow > day0_fast) and (day1_slow < day1_fast):
            return Signal.BUY
        if (day0_slow < day0_fast) and (day1_slow > day1_fast):
            return Signal.SELL
        return Signal.KEEP
