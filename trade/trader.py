import abc
from abc import abstractmethod

from sqlalchemy.testing.plugin.plugin_base import logging

from utils.common import *
from utils.redis_tool import RedisTool


class Position:
    def __init__(self, total_budget, max_hold):
        # 每个槽位的4个元素表示：
        ## 0-股票id
        ## 1-购入价格
        ## 2-购入数量
        ## 3-自定义字段
        self.hold = []
        for i in range(0, max_hold):
            self.hold.append([None, None, None, None])
        self.spare = total_budget
        self.max_hold = max_hold

    def get_slot(self, stock_id):
        for slot in self.hold:
            if stock_id == slot[0]:
                return slot
        return None

    def get_empty_slot(self):
        for slot in self.hold:
            if slot[0] is None:
                return slot
        return None


class Trader(abc.ABC):
    """
    注意，buy和sell函数，很可能依赖回调，所以注意不要实现/使用它们的返回值
    """

    def __init__(self, total_budget, max_hold):
        self.position = Position(total_budget, max_hold)

    @abstractmethod
    def buy(self, stock_id, budget, exp_price=None, dt=None):
        pass

    @abstractmethod
    def sell(self, stock_id, exp_price=None, dt=None):
        pass


class BacktestTrader(Trader):
    def __init__(self, total_budget, max_hold):
        super().__init__(total_budget, max_hold)
        self.backtest_db_no = conf_dict['Backtest']['Backtest_DB_NO']
        self.cache_tool = RedisTool(conf_dict['Redis']['Host'], conf_dict['Redis']['Port'],
                                    conf_dict['Redis']['Passwd'])

    @classmethod
    def trade_cost(cls, sig, money):
        # 佣金（包括规费）,最低5元
        commission = money * 0.0002
        if commission < 5:
            commission = 5
        # 过户费
        transfer_fee = money * 0.00001
        # 印花税（只卖出时征收）
        stamp_tax = 0
        if sig == TradeSide.SELL:
            stamp_tax = money * 0.0005
        logging.info("Cost Detail[" + str(commission) + "][" + str(transfer_fee) + "][" + str(stamp_tax) + "]")
        return commission + transfer_fee + stamp_tax

    def buy(self, stock_id, budget, exp_price=None, dt=None):
        position = self.position
        # 以市价买入，在回测时采用当天的开盘价
        if exp_price is None:
            price = self.cache_tool.get(stock_id, self.backtest_db_no, True)
            if (price is None) or (dt not in price.index):
                logging.error("No price in date[" + str(dt) + "] for stock[" + stock_id + "]")
                return
            exp_price = price.loc[dt, 'open']
        volume = int(budget / (exp_price * 100)) * 100
        if volume == 0:
            logging.info("Too Expensive because of PRICE, Fail to Buy")
            return
        money = exp_price * volume
        cost = BacktestTrader.trade_cost(TradeSide.BUY, money)
        if money + cost > position.spare:
            logging.info("Too Expensive because of COST, Fail to Buy")
            return
        slot = position.get_empty_slot()
        if slot is None:
            logging.info("Slot is Full, Fail to Buy")
            return
        slot[0] = stock_id
        slot[1] = exp_price
        slot[2] = volume
        position.spare -= (money + cost)
        logging.info(
            "Buy " + stock_id + ", Price " + str(exp_price) + ", Volume " + str(volume) + ", Total Price " + str(money))
        logging.info("Trade Cost is :[" + str(cost) + "]")
        logging.info("Spare is :[" + str(position.spare) + "]")

    def sell(self, stock_id, exp_price=None, dt=None):
        position = self.position
        slot = position.get_slot(stock_id)
        if slot is None:
            logging.info("No StockID " + stock_id)
            return
        volume = slot[2]
        # 以市价卖出，在回测时采用当天的开盘价
        if exp_price is None:
            price = self.cache_tool.get(stock_id, self.backtest_db_no, True)
            if (price is None) or (dt not in price.index):
                logging.error("No price in date[" + str(dt) + "] for stock[" + stock_id + "]")
                return
            exp_price = price.loc[dt, 'open']
        money = exp_price * volume
        cost = BacktestTrader.trade_cost(TradeSide.SELL, money)
        slot[0] = None
        slot[1] = None
        slot[2] = None
        position.spare += (money - cost)
        logging.info(
            "Sell " + stock_id + ", Price " + str(exp_price) + ", Volume " + str(volume) + ", Total Price " + str(
                money))
        logging.info("Trade Cost is :[" + str(cost) + "]")
        logging.info("Spare is :[" + str(position.spare) + "]")
