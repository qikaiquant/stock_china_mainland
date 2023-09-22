import random
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
    def __init__(self, sdt, edt, dbt, ct, cno, total_budget, max_hold, stg_id):
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
        # 设置止损止盈点
        self.stop_loss_point = -1  # 止损点，-1表示不设置
        self.stop_surplus_point = -1  # 止盈点，-1表示不设置
        if "StopLossPoint" in conf_dict['STG'][stg_id]:
            self.stop_loss_point = conf_dict['STG'][stg_id]['StopLossPoint']
        if "StopSurplusPoint" in conf_dict['STG'][stg_id]:
            self.stop_surplus_point = conf_dict['STG'][stg_id]['StopSurplusPoint']

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

    def _fill_daily_status(self, dt, action_log):
        nw = self.position.spare
        action_log['Spare'] = nw
        for stock_id, (_, volumn) in self.position.hold.items():
            price = self.cache_tool.get(stock_id, self.cache_no, serialize=True)
            nw += volumn * price.loc[dt, 'close']
            action_log['Hold'].append((stock_id, price.loc[dt, 'close'], volumn, volumn * price.loc[dt, 'close']))
        self.daily_status.loc[dt] = [nw, action_log]

    def stop_loss_surplus(self, stock_id, jiage):
        if stock_id not in self.position.hold:
            return False
        buy_jiage = self.position.hold[stock_id][0]
        if self.stop_loss_point != -1:
            min_jiage = buy_jiage * (100 - self.stop_loss_point) / 100.0
            if jiage < min_jiage:
                logging.info(
                    "Stop Loss For Stock " + stock_id + " At Price[" + str(jiage) + "](Buy:[" + str(buy_jiage) + "])")
                return True
        if self.stop_surplus_point != -1:
            max_jiage = buy_jiage * (100 + self.stop_loss_point) / 100.0
            if jiage > max_jiage:
                logging.info(
                    "Stop Surplus For Stock " + stock_id + " At Price[" + str(jiage) + "](Buy:[" + str(
                        buy_jiage) + "])")
                return True
        return False

    def survey(self, stocks, is_draw):
        # 如果不显式传入股票代码，则随机选择30支股票做调研
        if (stocks is None) or (len(stocks) == 0):
            stocks = self.cache_tool.get(RAND_STOCK, COMMON_CACHE_ID, serialize=True)
            if not stocks:
                stocks = random.sample(self.all_stocks, 30)
                self.cache_tool.set(RAND_STOCK, stocks, COMMON_CACHE_ID, serialize=True)
        # 调研过程
        for stock_id in stocks:
            logging.info("+++++++++++++++++++" + stock_id + "++++++++++++++++++++")
            price = self.cache_tool.get(stock_id, self.cache_no, serialize=True)
            if price is None:
                continue
            status = 1  # 1:空仓，2：满仓
            trade_pots = []
            for dt in self.bt_tds:
                sig_action = Signal.KEEP.name
                [pre_dt] = get_preN_tds(self.all_trade_days, dt, 1)
                if (dt not in price.index) or (pre_dt not in price.index):
                    continue
                signal, reason = self.signal(stock_id, pre_dt, price)
                cur_price = price.loc[dt, 'open']
                # 寻找交易信号
                if (status == 1) and signal == Signal.BUY:
                    trade_pots.append((dt, cur_price, "B"))
                    sig_action = Signal.BUY.name
                    status = 2
                elif (status == 2) and signal == Signal.SELL:
                    trade_pots.append((dt, cur_price, "S"))
                    sig_action = Signal.SELL.name
                    status = 1
                if sig_action == "KEEP" and reason == "Nothing":
                    continue
                logging.info("[%s][%s][%s]" % (str(dt), sig_action, reason))
            self.draw_survey(stock_id, price.loc[self.bt_sdt:self.bt_edt], trade_pots, is_draw)

    def backtest(self):
        # 载入benchmark
        self.cache_tool.set(BENCHMARK_KEY, self.daily_benchmark, COMMON_CACHE_ID, serialize=True)
        # 遍历所有回测交易日
        for dt in self.bt_tds:
            print(dt)
            action_log = {'Buy': [], 'Sell': [], 'Hold': []}
            position = self.position
            [pre_dt] = get_preN_tds(self.all_trade_days, dt, 1)
            # Check当前Hold是否需要卖出
            for stock_id in list(position.hold.keys()):
                price = self.cache_tool.get(stock_id, self.cache_no, serialize=True)
                if self.signal(stock_id, pre_dt, price) == Signal.SELL:
                    position.sell(stock_id, price.loc[dt, 'open'], sell_all=True)
                    action_log['Sell'].append((stock_id, price.loc[dt, 'open']))
            # 不满仓，补足
            if position.can_buy():
                # 遍历所有股票，补足持仓
                candidate = []
                for stock in self.all_stocks:
                    price = self.cache_tool.get(stock, self.cache_no, serialize=True)
                    if (dt not in price.index) or (pre_dt not in price.index):
                        continue
                    if self.signal(stock, pre_dt, price) == Signal.BUY:
                        candidate.append((stock, price.loc[pre_dt, 'money'], price.loc[dt, 'open']))
                candidate.sort(key=lambda x: x[1], reverse=True)
                for can in candidate:
                    if position.buy(can[0], can[2]):
                        action_log['Buy'].append((can[0], can[2]))
                    if not position.can_buy():
                        break
            self._fill_daily_status(dt, action_log)
        self.cache_tool.set(RES_KEY, self.daily_status, COMMON_CACHE_ID, serialize=True)

    def draw_survey(self, stock_id, price, pots, is_draw):
        """
        这个函数需要被子类重写
        :param stock_id:
        :param price:
        :param pots:
        :param is_draw:
        :return:
        """
        print("This is Base draw_survery().IF you don't rewirte it,NOTHING will happend.")
        pass

    def signal(self, stock_id, dt, price):
        """
        这个函数需要被子类重写
        :param stock_id:
        :param dt:
        :param price:
        :return:
        """
        print("This is Base Signal().IF you don't rewirte it,NOTHING will happend.")
        return Signal.KEEP

    def run(self):
        """
        这个函数需要被子类重写
        :return:
        """
        print("This is Base Run().IF you don't rewirte it,NOTHING will happend.")
        pass
