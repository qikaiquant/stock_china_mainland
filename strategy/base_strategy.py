import operator
import pandas

from utils.common import *


class Position:
    def __init__(self, bib):
        self.hold = {}
        self.spare = bib

    def buy(self, stock_id, price, budget):
        volumn = int(budget / (price * 100)) * 100
        if volumn == 0:
            logging.info("Too Expensive, Fail to Buy")
            return
        money = price * volumn
        if money > self.spare:
            logging.info("Cannot buy, No Enough Money")
            return
        if stock_id not in self.hold:
            self.hold[stock_id] = [price, volumn]
        else:
            new_volumn = volumn + self.hold[stock_id][2]
            new_price = (money + self.hold[stock_id][1] * self.hold[stock_id][2]) / new_volumn
            self.hold[stock_id].append(new_price, new_volumn)
        self.spare -= money

    def sell(self, stock_id, price, volumn=None, sell_all=False):
        if stock_id not in self.hold:
            logging.info("Nothing to Be Sold.")
            return
        cur_volumn = self.hold[stock_id][1]
        if sell_all or volumn >= cur_volumn:
            del self.hold[stock_id]
            self.spare += price * cur_volumn
        else:
            self.hold[stock_id][1] -= volumn
            self.spare += price * volumn


class STGContext:
    def __init__(self, sdt=None, edt=None, dbt=None, ct=None, bib=100000):
        # 存储连接工具
        self.db_tool = dbt
        self.cache_tool = ct
        # 回测相关字段
        self.bt_sdt = sdt
        self.bt_edt = edt
        self.bt_init_budget = bib
        self.bt_res = pandas.DataFrame()  # 包含基准/策略结果
        # 持仓明细
        self.ps = Position(bib=bib)


class BaseStrategy:
    def __init__(self, ctx):
        self.ctx = ctx

    def load_benchmark(self, bm_list):
        # 读入回测日期内的Benchmark
        sdt = self.ctx.bt_sdt
        edt = self.ctx.bt_edt
        bms = {}
        for bm_name in bm_list:
            res = self.ctx.db_tool.get_price(bm_name, ['dt', 'close'], sdt, edt)
            bms[bm_name] = pandas.DataFrame(res, columns=['dt', 'close'])
        # 验证几条Benchmark日期是否对齐，若对不齐抛RuntimError异常
        dt_valid = True
        pre_dt = None
        for name, bf in bms.items():
            if not pre_dt:
                pre_dt = bf['dt']
                continue
            if not operator.eq(pre_dt, bf['bt']):
                dt_valid = False
                break
        if not dt_valid:
            raise RuntimeError("BenchMark Error")
        self.ctx.bt_res['dt'] = pre_dt
        # 处理Bib并生成基线点
        for name, bf in bms.items():
            factor = self.ctx.bt_init_budget / bf['close'][0]
            self.ctx.bt_res['bm_' + name] = bf['close'] * factor
