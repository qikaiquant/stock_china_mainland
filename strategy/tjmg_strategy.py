import numpy as np

from strategy.base_strategy import BaseStrategy
from utils.common import *


class TDStatus(Enum):
    TJ = 0,
    MG = 1


class TJMGStrategy(BaseStrategy):
    def __init__(self, stg_id, trader):
        super().__init__(stg_id, trader)
        # 偷鸡/摸狗标志
        self.td_status = TDStatus.MG
        self.tj_start_day = None
        # 覆盖掉全量股票池
        self.all_stocks = self.db_tool.get_stock_info(['stock_id', 'start_date', 'end_date'])
        # 中小板综指信息(399101)
        self.middle_small_stocks = {}
        for (stock_id, start_dt, end_dt) in self.all_stocks:
            if stock_id.startswith(("002", "003")):
                self.middle_small_stocks[stock_id] = (start_dt, end_dt)

    def check_TJ(self, dt):
        d2, d1 = get_preN_tds(self.all_trade_days, dt, 2)
        sort_array = []
        for stock_id in self.middle_small_stocks.keys():
            res = self.db_tool.get_valuation_st(stock_id, ['circulating_market_cap'], start_dt=d1, end_dt=d2)
            if len(res) != 2:
                continue
            pre_dt_cmc = res[0][0]
            dt_cmc = res[1][0]
            sort_array.append((pre_dt_cmc, dt_cmc))
        sort_array.sort(key=lambda x: x[1], reverse=False)
        top20_array = []
        for i in range(0, 20):
            a = (sort_array[i][1] / sort_array[i][0] - 1) * 100
            top20_array.append(a)
        top20_np_array = np.array(top20_array)
        a2 = np.linalg.norm(top20_np_array)
        top20_normalized_np_array = top20_np_array / a2
        var = np.var(top20_normalized_np_array)
        mean = np.mean(top20_normalized_np_array)
        if mean > 0 and var < 0.02:
            return True
        return False

    def build_stock_bool(self, dt):
        pool = []
        stocks_set = set()
        # 回溯250个交易日，上市日期必须在该日期之前
        list_ddl = get_preN_tds(self.all_trade_days, dt, 250)[-1]
        for stock in self.all_stocks:
            stock_id = stock[0]
            listed_day = stock[1]
            delisted_day = stock[2]
            # 滤除指数
            if stock_id in BENCH_MARK:
                continue
            # 滤除已退市股票
            if delisted_day < dt:
                continue
            # 滤除上市不满250个交易日的股票
            if listed_day > list_ddl:
                continue
            # 滤除创业板、科创板、北交所股票
            if stock_id.startswith(('68', '4', '8', '3')):
                continue
            # 滤除st股票
            is_st, market_cap = self.db_tool.get_valuation_st(stock_id, ['st', "market_cap"], start_dt=dt, end_dt=dt)[0]
            if is_st == 1:
                continue
            # 滤除股价超过10块的股票
            close_price = self.db_tool.get_price(stock_id, ['close'], start_dt=dt, end_dt=dt)[0][0]
            if close_price > 10:
                continue
            # 滤除roa/roe达不到阈值的股票（注意不要引入未来函数）
            q = 10 * dt.year + get_quarter(dt)
            pre_q = 10 * (dt.year - 1) + get_quarter(dt)
            res = list(self.db_tool.get_indicator(stock_id, ['quarter', 'publish_dt', 'roe', 'roa'], pre_q, q))
            cur_roe = cur_roa = None
            while len(res) != 0:
                (q, pub_dt, roe, roa) = res.pop()
                if pub_dt > dt:
                    continue
                cur_roa = roa
                cur_roe = roe
                break
            if cur_roe is None or cur_roa is None:
                continue
            if cur_roe < 0.15 or cur_roa < 0.1:
                continue
            pool.append((stock_id, market_cap))
            stocks_set.add(stock_id)
        pool.sort(key=lambda x: x[1], reverse=False)
        return pool, stocks_set

    def adjust_position(self, dt):
        pre_dt = get_preN_tds(self.all_trade_days, dt, 1)[0]
        trader = self.trader
        position = trader.position
        # 取到前一天涨停列表
        limit_up_list = []
        print("Start Prepare Limit up List")
        for (stock_id, ipo_dt, delist_dt) in self.all_stocks:
            if pre_dt < ipo_dt or pre_dt > delist_dt:
                continue
            close_p, limit_p_p, is_paused = self.db_tool.get_price(stock_id, ["close", 'high_limit', "paused"],
                                                                   start_dt=pre_dt, end_dt=pre_dt)[0]
            if is_paused == 1:
                continue
            if 100 * (limit_p_p - close_p) / close_p < 0.1:
                limit_up_list.append(stock_id)
        print(len(limit_up_list))
        # 状态机
        if self.td_status == TDStatus.MG:
            print("Status MG")
            if self.check_TJ(dt):
                print("Transfer to TJ")
                # 偷鸡状态
                # 建池子
                pool, stocks_set = self.build_stock_bool(dt)
                # 卖
                for slot in position.hold:
                    stock_id = slot[0]
                    if stock_id is None:
                        continue
                    if stock_id not in stocks_set:
                        print("Sell " + stock_id)
                        trader.sell(stock_id, dt=dt)
                # 买
                empty_count = position.max_hold - position.get_hold_count()
                budget = position.spare / empty_count
                while True:
                    (stock_id, _) = pool.pop(0)
                    print("Buy " + stock_id)
                    trader.buy(stock_id, budget=budget, dt=dt)
                    if position.get_hold_count() == position.max_hold:
                        break
                self.td_status = TDStatus.TJ
                self.tj_start_day = dt
                return  # 偷鸡日第一天，建好仓就完事了
        elif self.td_status == TDStatus.TJ:
            print("Status TJ")
            if (dt - self.tj_start_day).days >= 30:
                # 清仓所有股票
                for slot in position.hold:
                    stock_id = slot[0]
                    if stock_id is None:
                        continue
                    print("Sell " + stock_id)
                    trader.sell(stock_id, dt=dt)
                self.td_status = TDStatus.MG
                self.tj_start_day = None
                return
        # 处理涨停
        # 卖
        print("Daily Handle")
        for slot in position.hold:
            stock_id = slot[0]
            if stock_id is None:
                continue
            cur_price = trader.get_current_price(stock_id, dt=dt)
            if stock_id in limit_up_list:
                print("Sell 1 " + stock_id)
                trader.sell(stock_id, dt=dt)
            if self.stop_loss_surplus(stock_id, cur_price):
                print("Sell 2 " + stock_id)
                trader.sell(stock_id, dt=dt)
            print("Keep " + stock_id)
        # 买 TODO 这条策略值得怀疑，先空置，后续check下合理性

    def survey(self):
        start_dt = datetime.strptime('2023-06-28', '%Y-%m-%d').date()
        end_dt = datetime.strptime('2023-08-01', '%Y-%m-%d').date()
        for dt in self.all_trade_days:
            if start_dt <= dt <= end_dt:
                print(str(dt))
                self.adjust_position(dt)
