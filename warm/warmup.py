import abc
import traceback

import pandas

from utils.common import *
from utils.db_tool import DBTool
from utils.redis_tool import RedisTool


class BaseWarmer(abc.ABC):
    def __init__(self, stg_id=None):
        self.db_tool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                              conf_dict['Mysql']['Passwd'])
        self.cache_tool = RedisTool(conf_dict['Redis']['Host'], conf_dict['Redis']['Port'],
                                    conf_dict['Redis']['Passwd'])
        if stg_id is not None:
            self.cache_no = conf_dict['STG'][stg_id]['DB_NO']
        self.warm_start_date = datetime.strptime(conf_dict['STG']["Base"]['Warm_Start_Date'], '%Y-%m-%d').date()
        self.warm_end_date = datetime.strptime(conf_dict['STG']["Base"]['Warm_End_Date'], '%Y-%m-%d').date()

    @abc.abstractmethod
    def warm(self):
        """
        这个函数需要子类重写
        :return:
        """
        pass


class TJMGWarmer(BaseWarmer):
    @staticmethod
    def join_res(price_res, val_res):
        join_dict = {}
        for (dt, close, high_limit, paused) in price_res:
            join_dict[dt] = [close, high_limit, paused, None, None, None]
        for (dt, circulating_market_cap, st, market_cap) in val_res:
            if dt not in join_dict:
                join_dict[dt] = [None, None, None, circulating_market_cap, st, market_cap]
            else:
                join_dict[dt][3] = circulating_market_cap
                join_dict[dt][4] = st
                join_dict[dt][5] = market_cap
        return join_dict

    def warm(self):
        self.cache_tool.clear(int(self.cache_no))
        stocks = self.db_tool.get_stock_info(['stock_id'])
        for (stock_id,) in stocks:
            if stock_id in BENCH_MARK:
                continue
            try:
                price_cols = ["dt", "close", 'high_limit', "paused"]
                val_cols = ["dt", "circulating_market_cap", "st", "market_cap"]
                price_res = self.db_tool.get_price(stock_id, fields=price_cols, start_dt=self.warm_start_date,
                                                   end_dt=self.warm_end_date)
                val_res = self.db_tool.get_valuation_st(stock_id, fields=val_cols, start_dt=self.warm_start_date,
                                                        end_dt=self.warm_end_date)

                if len(price_res) == 0 and len(val_res) == 0:
                    continue
                merged_res = self.join_res(price_res, val_res)
                for k, v in merged_res.items():
                    redis_key = stock_id + ":" + str(k)
                    self.cache_tool.set(redis_key, v, self.cache_no, serialize=True)
            except Exception as e:
                traceback.print_exc()
                break


class BacktestWarmer(BaseWarmer):
    def __init__(self, stg_id=None):
        super().__init__(stg_id)
        self.cache_no = conf_dict["Backtest"]["Backtest_DB_NO"]

    def warm(self):
        self.cache_tool.clear(int(self.cache_no))
        stocks = self.db_tool.get_stock_info(['stock_id'])
        cols = ['dt', 'close', 'open', 'low', 'high', 'avg']
        for (stock_id,) in stocks:
            try:
                # 预热全量股票行情
                res = self.db_tool.get_price(stock_id, fields=cols, start_dt=self.warm_start_date,
                                             end_dt=self.warm_end_date)
                ## 股票在2013-01-01前已退市
                if len(res) == 0:
                    continue
                res_df = pandas.DataFrame(res, columns=cols)
                res_df.set_index('dt', inplace=True)
                self.cache_tool.set(stock_id, res_df, self.cache_no, serialize=True)
            except Exception as e:
                traceback.print_exc()
                break
