import operator
import pandas


class STGContext:
    def _expand_trads_days(self, sdt, edt):
        tds = []
        res = self.db_tool.get_trade_days(sdt, edt)
        for (td,) in res:
            tds.append(td)
        return tds

    def __init__(self, sdt=None, edt=None, dbt=None, ct=None, money=100000):
        # 存储连接工具
        self.db_tool = dbt
        self.cache_tool = ct
        # 回测日期相关字段
        self.bt_sdt = sdt
        self.bt_edt = edt
        self.bt_tds = self._expand_trads_days(sdt, edt)
        # 持仓相关字段
        self.position = pandas.DataFrame()


class BaseStrategy:
    def __init__(self, ctx):
        self.ctx = ctx

    def backtest(self):
        """
        所有子类都必须实现该方法
        :return:
        """
        pass

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
