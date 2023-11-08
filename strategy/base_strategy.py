from enum import Enum

import pandas

from utils.common import *
from utils.db_tool import DBTool
from utils.redis_tool import RedisTool

# 策略结果Key
RES_KEY = "RES_KEY:"
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


class BenchMark(Enum):
    HS300 = "000300.XSHG"  # 沪深300


class Position:
    def __init__(self, bib, mh, tc_switch):
        self.hold = {}
        self.spare = bib
        self.max_hold = mh
        self._budget = float(bib / mh)
        self._trade_cost_switch = tc_switch

    def can_buy(self):
        if self.max_hold > len(self.hold):
            return True
        return False

    def _trade_cost(self, action, money):
        if not self._trade_cost_switch:
            return 0
        # 佣金（包括规费）,最低5元
        commission = money * 0.0002
        if commission < 5:
            commission = 5
        # 过户费
        transfer_fee = money * 0.00001
        # 印花税（只卖出时征收）
        stamp_tax = 0
        if action == Signal.SELL:
            stamp_tax = money * 0.0005
        logging.info("Cost Detail[" + str(commission) + "][" + str(transfer_fee) + "][" + str(stamp_tax) + "]")
        return commission + transfer_fee + stamp_tax

    def buy(self, stock_id, jiage):
        if len(self.hold) >= self.max_hold:
            logging.info("Too Many Holds!")
            return False
        budget = self._budget if self._budget < self.spare else self.spare
        volumn = int(budget / (jiage * 100)) * 100
        if volumn == 0:
            logging.info("Too Expensive because of PRICE, Fail to Buy")
            return False
        money = jiage * volumn
        cost = self._trade_cost(Signal.BUY, money)
        if money + cost > self.spare:
            logging.info("Too Expensive because of COST, Fail to Buy")
            return False
        if stock_id not in self.hold:
            self.hold[stock_id] = [jiage, volumn]
        else:
            new_volumn = volumn + self.hold[stock_id][2]
            new_jiage = (money + self.hold[stock_id][1] * self.hold[stock_id][2]) / new_volumn
            self.hold[stock_id].append(new_jiage, new_volumn)
        self.spare -= (money + cost)
        logging.info("Trader Cost is :[" + str(cost) + "]")
        return True

    def sell(self, stock_id, jiage, volumn=None, sell_all=False):
        if stock_id not in self.hold:
            logging.info("Nothing to Be Sold.")
            return
        before = self.spare
        cur_volumn = self.hold[stock_id][1]
        if sell_all or volumn >= cur_volumn:
            del self.hold[stock_id]
            self.spare += jiage * cur_volumn
        else:
            self.hold[stock_id][1] -= volumn
            self.spare += jiage * volumn
        cost = self._trade_cost(Signal.SELL, (self.spare - before))
        self.spare -= cost
        logging.info("Trader Cost is :[" + str(cost) + "]")
        if self.spare < 0:
            logging.error("Spare Below ZERO, GAME OVER")


class BaseStrategy:
    def __init__(self, stg_id):
        self.stg_id = stg_id
        # 存储定义
        self.db_tool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                              conf_dict['Mysql']['Passwd'])
        self.cache_tool = RedisTool(conf_dict['Redis']['Host'], conf_dict['Redis']['Port'],
                                    conf_dict['Redis']['Passwd'])
        self.cache_no = conf_dict['STG'][stg_id]['DB_NO']
        # 策略参数
        self._bt_sdt = conf_dict['Backtest']['Start_Date']
        self._bt_edt = conf_dict['Backtest']['End_Date']
        self.bt_tds = self.db_tool.get_trade_days(self._bt_sdt, self._bt_edt)
        self.stop_loss_point = conf_dict['STG']['Base']['StopLossPoint']  # 止损点，-1表示不设置
        self.stop_surplus_point = conf_dict['STG']['Base']['StopSurplusPoint']  # 止盈点，-1表示不设置
        self.total_budget = conf_dict['Backtest']['Budget']
        self.max_hold = conf_dict['STG']["Base"]['MaxHold']
        # 持仓动态
        self.position = Position(self.total_budget, self.max_hold, conf_dict['Backtest']['Trade_Cost_Switch'])  # 持仓变化
        self.daily_benchmark = self._init_daily_benchmark()  # 分日明细
        self.daily_status = pandas.DataFrame(columns=['dt', 'stg_nw', 'details'])
        self.daily_status.set_index('dt', inplace=True)
        # 股票/交易日全量信息
        self.all_trade_days = self.db_tool.get_trade_days()
        self.all_stocks = []
        res = self.db_tool.get_stock_info(['stock_id'])
        for (sid,) in res:
            self.all_stocks.append(sid)

    def _init_daily_benchmark(self):
        df = pandas.DataFrame()
        # 添加每列
        for bm in BenchMark:
            res = self.db_tool.get_price(bm.value, ['dt', 'close'], self._bt_sdt, self._bt_edt)
            factor = float(self.total_budget / res[0][1])
            bmdf = pandas.DataFrame(res, columns=['dt', 'jiage'])
            df['dt'] = bmdf['dt']
            df[bm.name] = bmdf['jiage'] * factor
        df.set_index('dt', inplace=True)
        return df

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
            max_jiage = buy_jiage * (100 + self.stop_surplus_point) / 100.0
            if jiage > max_jiage:
                logging.info(
                    "Stop Surplus For Stock " + stock_id + " At Price[" + str(jiage) + "](Buy:[" + str(
                        buy_jiage) + "])")
                return True
        return False

    def backtest(self, pid=""):
        # 载入benchmark
        self.cache_tool.set(BENCHMARK_KEY, self.daily_benchmark, COMMON_CACHE_ID, serialize=True)
        # 遍历所有回测交易日
        for dt in self.bt_tds:
            logging.info("+++++++++++++++++++" + str(dt) + "++++++++++++++++++++")
            action_log = {'Buy': [], 'Sell': [], 'Hold': []}
            position = self.position
            [pre_dt] = get_preN_tds(self.all_trade_days, dt, 1)
            # Check当前Hold是否需要卖出
            for stock_id in list(position.hold.keys()):
                price = self.cache_tool.get(stock_id, self.cache_no, serialize=True)
                signal, _ = self.signal(stock_id, pre_dt, price)
                if signal == Signal.SELL:
                    position.sell(stock_id, price.loc[dt, 'open'], sell_all=True)
                    action_log['Sell'].append((stock_id, price.loc[dt, 'open']))
            # 不满仓，补足
            if position.can_buy():
                # 遍历所有股票，补足持仓
                candidate = []
                for stock_id in self.all_stocks:
                    price = self.cache_tool.get(stock_id, self.cache_no, serialize=True)
                    if (price is None) or (dt not in price.index) or (pre_dt not in price.index):
                        continue
                    signal, _ = self.signal(stock_id, pre_dt, price)
                    if signal == Signal.BUY:
                        candidate.append((stock_id, price.loc[pre_dt, 'money'], price.loc[dt, 'open']))
                candidate.sort(key=lambda x: x[1], reverse=True)
                for can in candidate:
                    if position.buy(can[0], can[2]):
                        action_log['Buy'].append((can[0], can[2]))
                    if not position.can_buy():
                        break
            self._fill_daily_status(pre_dt, action_log)
        self.cache_tool.set(RES_KEY + self.stg_id + ":" + pid, self.daily_status, COMMON_CACHE_ID, serialize=True)

    def build_param_space(self):
        """
        这个函数需要被子类重写
        :return:
        """
        print("This is Base build_param_space().IF you don't rewrite it,NOTHING will happen.")

    def reset_param(self, param):
        """
        这个函数需要被子类重写
        :param param:
        :return:
        """
        print("This is Base reset_param().IF you don't rewrite it,NOTHING will happen.")

    def survey(self, stocks, is_draw):
        """
        这个函数需要被子类重写
        :param stocks:
        :param is_draw:
        :return:
        """
        print("This is Base survey().IF you don't rewrite it,NOTHING will happen.")

    def signal(self, stock_id, dt, price):
        """
        这个函数需要被子类重写
        :param stock_id:
        :param dt:
        :param price:
        :return:
        """
        print("This is Base Signal().IF you don't rewrite it,NOTHING will happen.")
        return Signal.KEEP
