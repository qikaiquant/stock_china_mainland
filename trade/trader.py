import abc
from abc import abstractmethod

from sqlalchemy.testing.plugin.plugin_base import logging

from utils.common import *
from utils.redis_tool import RedisTool


class Position:
    def __init__(self, total_budget, max_hold):
        # 每个槽位的4个元素表示：0-股票id，1-购入价格，2-购入数量，3-状态
        self.hold = []
        for i in range(0, max_hold):
            self.hold.append([None, None, None, PositionStatus.INIT])
        self.spare = total_budget
        self.max_hold = max_hold
        self.single_budget = float(total_budget / max_hold)
        self.trader = None

    def get_slot(self, stock_id):
        for slot in self.hold:
            if stock_id == slot[0]:
                return slot
        return None

    def set_trader(self, trader):
        self.trader = trader


class Trader(abc.ABC):
    """
    注意，buy和sell函数，很可能依赖回调，所以注意不要实现/使用它们的返回值
    """

    @abstractmethod
    def buy(self, position, slot, dt, stock_id, jiage=None):
        pass

    @abstractmethod
    def sell(self, position, slot, dt, jiage=None):
        pass


class Backtest_Trader(Trader):
    def __init__(self):
        self.price_db_no = conf_dict['Backtest']['Price_DB_NO']
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
        if sig == Signal.SELL:
            stamp_tax = money * 0.0005
        logging.debug("Cost Detail[" + str(commission) + "][" + str(transfer_fee) + "][" + str(stamp_tax) + "]")
        return commission + transfer_fee + stamp_tax

    def buy(self, position, slot, dt, stock_id, jiage=None):
        # 以市价买入，在回测时采用当天的开盘价
        if jiage is None:
            price = self.cache_tool.get(stock_id, self.price_db_no, True)
            if (price is None) or (dt not in price.index):
                logging.error("No price in date[" + str(dt) + "] for stock[" + stock_id + "]")
                slot[3] = PositionStatus.BUY_FAIL
                return
            jiage = price.loc[dt, 'open']
        budget = position.single_budget if position.single_budget < position.spare else position.spare
        volume = int(budget / (jiage * 100)) * 100
        if volume == 0:
            logging.info("Too Expensive because of PRICE, Fail to Buy")
            slot[3] = PositionStatus.BUY_FAIL
            return
        money = jiage * volume
        cost = Backtest_Trader.trade_cost(Signal.BUY, money)
        if money + cost > position.spare:
            logging.info("Too Expensive because of COST, Fail to Buy")
            slot[3] = PositionStatus.BUY_FAIL
            return
        slot[0] = stock_id
        slot[1] = jiage
        slot[2] = volume
        slot[3] = PositionStatus.KEEP
        position.spare -= (money + cost)
        logging.info("Trade Cost is :[" + str(cost) + "]")

    def sell(self, position, slot, dt, jiage=None):
        # 以市价卖出，在回测时采用当天的开盘价
        stock_id = slot[0]
        if jiage is None:
            price = self.cache_tool.get(stock_id, self.price_db_no, True)
            if (price is None) or (dt not in price.index):
                logging.error("No price in date[" + str(dt) + "] for stock[" + stock_id + "]")
                slot[3] = PositionStatus.KEEP
                return
            jiage = price.loc[dt, 'open']
        money = jiage * slot[2]
        cost = Backtest_Trader.trade_cost(Signal.SELL, money)
        slot[0] = None
        slot[1] = None
        slot[2] = None
        slot[3] = PositionStatus.EMPTY
        position.spare += (money - cost)
        logging.info("Trade Cost is :[" + str(cost) + "]")
