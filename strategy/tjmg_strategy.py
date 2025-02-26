import numpy as np
from sqlalchemy.testing.plugin.plugin_base import logging

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
        # 覆盖掉全量股票池，去除掉benchmark
        self.all_stocks = self.db_tool.get_stock_info(['stock_id', 'start_date', 'end_date'], ex_benchmark=True)
        # 中小板综指信息(399101)
        self.middle_small_stocks = {}
        for (stock_id, start_dt, end_dt) in self.all_stocks:
            if stock_id.startswith(("002", "003")):
                self.middle_small_stocks[stock_id] = (start_dt, end_dt)

    def _get_cache(self, stock_id, dt, field):
        key = stock_id + ":" + str(dt)
        value = self.cache_tool.get(key, self.cache_no, True)
        if value is None or len(value) != 6:
            return None
        match field:
            case "close":
                return value[0]
            case "high_limit":
                return value[1]
            case "paused":
                return value[2]
            case "circulating_market_cap":
                return value[3]
            case "st":
                return value[4]
            case "market_cap":
                return value[5]
            case _:
                return None

    def check_TJ(self, dt):
        d2, d1 = get_preN_tds(self.all_trade_days, dt, 2)
        sort_array = []
        for stock_id in self.middle_small_stocks.keys():
            pre_dt_cmc = self._get_cache(stock_id, d1, "circulating_market_cap")
            dt_cmc = self._get_cache(stock_id, d2, "circulating_market_cap")
            if pre_dt_cmc is None or dt_cmc is None:
                continue
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
            is_st = self._get_cache(stock_id, dt, "st")
            market_cap = self._get_cache(stock_id, dt, "market_cap")
            if is_st == 1:
                continue
            # 滤除股价超过10块的股票
            close_price = self._get_cache(stock_id, dt, "close")
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
        logging.info("Start Prepare Limit up List")
        for (stock_id, ipo_dt, delist_dt) in self.all_stocks:
            if pre_dt < ipo_dt or pre_dt > delist_dt:
                continue
            close_p = self._get_cache(stock_id, pre_dt, "close")
            limit_p_p = self._get_cache(stock_id, pre_dt, "high_limit")
            is_paused = self._get_cache(stock_id, pre_dt, "paused")
            if is_paused == 1:
                continue
            if 100 * (limit_p_p - close_p) / close_p < 0.1:
                limit_up_list.append(stock_id)
        logging.info(len(limit_up_list))
        # 状态机
        if self.td_status == TDStatus.MG:
            logging.info("Status MG")
            if self.check_TJ(dt):
                logging.info("Transfer to TJ")
                # 偷鸡状态
                # 建池子
                pool, stocks_set = self.build_stock_bool(dt)
                # 卖
                for slot in position.hold:
                    stock_id = slot[0]
                    if stock_id is None:
                        continue
                    if stock_id not in stocks_set:
                        logging.info("Sell " + stock_id)
                        trader.sell(stock_id, dt=dt)
                # 买
                empty_count = position.max_hold - position.get_hold_count()
                budget = position.spare / empty_count
                while True:
                    (stock_id, _) = pool.pop(0)
                    logging.info("Buy " + stock_id)
                    trader.buy(stock_id, budget=budget, dt=dt)
                    if position.get_hold_count() == position.max_hold:
                        break
                self.td_status = TDStatus.TJ
                self.tj_start_day = dt
                return  # 偷鸡日第一天，建好仓就完事了
        elif self.td_status == TDStatus.TJ:
            logging.info("Status TJ")
            if (dt - self.tj_start_day).days >= 30:
                # 清仓所有股票
                for slot in position.hold:
                    stock_id = slot[0]
                    if stock_id is None:
                        continue
                    logging.info("Sell " + stock_id)
                    trader.sell(stock_id, dt=dt)
                self.td_status = TDStatus.MG
                self.tj_start_day = None
                return
        # 处理涨停
        # 卖
        logging.info("Daily Handle")
        for slot in position.hold:
            stock_id = slot[0]
            if stock_id is None:
                continue
            cur_price = trader.get_current_price(stock_id, dt=dt)
            if stock_id in limit_up_list:
                logging.info("Sell 1 " + stock_id)
                trader.sell(stock_id, dt=dt)
            if self.stop_loss_surplus(stock_id, cur_price):
                logging.info("Sell 2 " + stock_id)
                trader.sell(stock_id, dt=dt)
            logging.info("Keep " + stock_id)
        # 买 TODO 这条策略值得怀疑，先空置，后续check下合理性

    def survey(self):
        start_dt = datetime.strptime('2023-06-28', '%Y-%m-%d').date()
        end_dt = datetime.strptime('2023-08-01', '%Y-%m-%d').date()
        for dt in self.all_trade_days:
            if start_dt <= dt <= end_dt:
                logging.info(str(dt))
                self.adjust_position(dt)
