import abc
import traceback

import numpy
import pandas
import talib

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


class MacdWarmer(BaseWarmer):
    def warm(self):
        self.cache_tool.clear(int(self.cache_no))
        stocks = self.db_tool.get_stock_info(['stock_id', 'end_date'])
        cols = ['dt', 'close', 'open', 'money']
        for (stock_id, end_date) in stocks:
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
                self.cache_tool.set(DELIST_PRE + stock_id, end_date, self.cache_no, serialize=True)
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
        cols = ['dt', 'close', 'open', 'low', 'high']
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
