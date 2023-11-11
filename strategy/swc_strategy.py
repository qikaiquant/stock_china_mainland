import json
from datetime import datetime

import numpy
import pandas
import talib

from strategy.base_strategy import BaseStrategy, BaseWarmer


class SWCWarmer(BaseWarmer):

    def warm(self):
        self.cache_tool.clear(int(self.cache_no))
        stocks = self.db_tool.get_stock_info(['stock_id', 'ext'])
        cols = ['dt', 'close', 'open', 'money']
        start_dt = datetime.strptime('2018-01-01', '%Y-%m-%d').date()
        end_dt = datetime.today().date()
        # 载入个股行情
        for (stock_id, _) in stocks:
            res = self.db_tool.get_price(stock_id, fields=cols, start_dt=start_dt, end_dt=end_dt)
            # 股票start_dt前已退市
            if len(res) == 0:
                continue
            res_df = pandas.DataFrame(res, columns=cols)
            res_df.set_index('dt', inplace=True)
            res_df['roc'] = talib.ROC(numpy.array(res_df['close']), timeperiod=1)
            self.cache_tool.set(stock_id, res_df, self.cache_no, serialize=True)
        # 载入个股和申万分类的映射
        swc_map = {}
        for (stock_id, ext) in stocks:
            ext_info = json.loads(ext)
            if "SW_Code" not in ext_info:
                continue
            for swc in ext_info["SW_Code"]:
                if swc not in swc_map:
                    swc_map[swc] = [stock_id]
                else:
                    swc_map[swc].append(stock_id)
        # 计算末级分类下的P1,P2和STD
        trade_days = self.db_tool.get_trade_days(start_date=start_dt, end_date=end_dt)
        for swc, stock_ids in swc_map.items():
            df = pandas.DataFrame(columns=['dt', 'P1', 'P2', 'STD'])
            for dt in trade_days:
                if dt == start_dt:
                    df.loc[len(df)] = [dt, 0, 0, 0]
                    continue


class SWCStrategy(BaseStrategy):
    def __init__(self, stg_id):
        super().__init__(stg_id)

    def survey(self, stocks, is_draw):
        pass

    def signal(self, stock_id, dt, price):
        pass
