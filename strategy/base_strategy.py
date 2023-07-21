import pandas

from utils.common import *

# 策略结果Key
RES_KEY = "RES_KEY"
# 基准结果Key
BENCHMARK_KEY = "BENCHMARK_KEY"
# 调研用的随机Stock ID
RAND_STOCK = 'RAND_STOCK'

# 存放回测结果的Redis DB
COMMON_CACHE_ID = conf_dict["Redis"]["CommonCache"]


class Signal(Enum):
    BUY = 0
    SELL = 1
    KEEP = 2


class Position:
    def __init__(self, bib, mh):
        self.hold = {}
        self.spare = bib
        self.max_hold = mh
        self._budget = float(bib / mh)

    def can_buy(self):
        if self.max_hold > len(self.hold):
            return True
        return False

    def buy(self, stock_id, jiage):
        if len(self.hold) >= self.max_hold:
            logging.info("Too Many Holds!")
            return False
        budget = self._budget if self._budget < self.spare else self.spare
        volumn = int(budget / (jiage * 100)) * 100
        if volumn == 0:
            logging.info("Too Expensive, Fail to Buy")
            return False
        money = jiage * volumn
        if stock_id not in self.hold:
            self.hold[stock_id] = [jiage, volumn]
        else:
            new_volumn = volumn + self.hold[stock_id][2]
            new_jiage = (money + self.hold[stock_id][1] * self.hold[stock_id][2]) / new_volumn
            self.hold[stock_id].append(new_jiage, new_volumn)
        self.spare -= money
        return True

    def sell(self, stock_id, jiage, volumn=None, sell_all=False):
        if stock_id not in self.hold:
            logging.info("Nothing to Be Sold.")
            return
        cur_volumn = self.hold[stock_id][1]
        if sell_all or volumn >= cur_volumn:
            del self.hold[stock_id]
            self.spare += jiage * cur_volumn
        else:
            self.hold[stock_id][1] -= volumn
            self.spare += jiage * volumn


class BaseStrategy:

    def __init__(self, sdt=None, edt=None, dbt=None, ct=None, cno=None, total_budget=100000, max_hold=5):
        # 存储相关字段
        self.db_tool = dbt
        self.cache_tool = ct
        self.cache_no = cno
        # 回测日期相关字段
        self.bt_sdt = sdt
        self.bt_edt = edt
        self.bt_tds = self._expand_trads_days(sdt, edt)
        # 持仓相关字段
        self.total_budget = total_budget
        self.position = Position(total_budget, max_hold)  # 持仓变化
        self.daily_benchmark = self._init_daily_benchmark()  # 分日明细
        self.daily_status = pandas.DataFrame(columns=['dt', 'stg_nw', 'details'])
        self.daily_status.set_index('dt', inplace=True)
        # 载入股票全量信息
        self.all_stocks = []
        self.all_trade_days = []
        res = self.db_tool.get_stock_info(['stock_id'])
        for (sid,) in res:
            self.all_stocks.append(sid)
        res = self.db_tool.get_trade_days()
        for (td,) in res:
            self.all_trade_days.append(td)

    def _init_daily_benchmark(self):
        df = pandas.DataFrame()
        # 添加每列
        for bm in BenchMark:
            res = self.db_tool.get_price(bm.value, ['dt', 'close'], self.bt_sdt, self.bt_edt)
            factor = float(self.total_budget / res[0][1])
            bmdf = pandas.DataFrame(res, columns=['dt', 'jiage'])
            df['dt'] = bmdf['dt']
            df[bm.name] = bmdf['jiage'] * factor
        df.set_index('dt', inplace=True)
        return df

    def _expand_trads_days(self, sdt, edt):
        tds = []
        res = self.db_tool.get_trade_days(sdt, edt)
        for (td,) in res:
            tds.append(td)
        return tds

    def fill_daily_status(self, dt, action_log):
        nw = self.position.spare
        action_log['Spare'] = nw
        for stock_id, (_, volumn) in self.position.hold.items():
            price = self.cache_tool.get(stock_id, self.cache_no, serialize=True)
            nw += volumn * price.loc[dt, 'close']
            action_log['Hold'].append((stock_id, price.loc[dt, 'close'], volumn, volumn * price.loc[dt, 'close']))
        self.daily_status.loc[dt] = [nw, action_log]

    def backtest(self):
        """
        所有子类都必须实现该方法
        :return:
        """
        pass
