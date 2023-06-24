import logging
import operator

import pandas


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


class STGContext:

    def __init__(self, sdt=None, edt=None, dbt=None, ct=None, cno=None, total_budget=100000, max_hold=5):
        # 存储相关字段
        self.db_tool = dbt
        self.cache_tool = ct
        self.cache_no = cno
        # 回测日期相关字段
        self.bt_sdt = sdt
        self.bt_edt = edt
        self.bt_tds = self._expand_trads_days(sdt, edt)
        # 持仓明细
        self.position = Position(total_budget, max_hold)
        self.daily_nw = pandas.DataFrame(columns=['dt', 'stg_networth', 'details'])

    def fill_detail(self, dt, action_log):
        nw = self.position.spare
        action_log.append("@" + str(nw))
        for stock_id, (_, volumn) in self.position.hold.items():
            price = self.cache_tool.get(stock_id, self.cache_no, serialize=True)
            nw += volumn * price.loc[dt, 'close']
            action_log.append("$" + stock_id + "(" + str(price.loc[dt, 'close']) + "," + str(volumn) + "," + str(
                volumn * price.loc[dt, 'close']) + "," + ")")
        action_log.append("#" + str(nw))
        new_row = [dt, nw, str(action_log)]
        print(action_log)
        self.daily_nw.loc[len(self.daily_nw)] = new_row

    def _expand_trads_days(self, sdt, edt):
        tds = []
        res = self.db_tool.get_trade_days(sdt, edt)
        for (td,) in res:
            tds.append(td)
        return tds


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
