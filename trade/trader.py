import abc
from abc import abstractmethod
from enum import auto

from utils.common import *


class SlotStatus(Enum):
    Keep = auto,
    Buying = auto,
    Selling = auto


class Slot:
    def __init__(self, stock_id, price, volume, avail_volume, status):
        self.stock_id = stock_id
        self.price = price
        self.volume = volume
        self.avail_volume = avail_volume
        self.status = status

    def output_info(self):
        logging.info("Stock Code:" + self.stock_id)
        logging.info("Price:" + str(self.price))
        logging.info("Volume:" + str(self.volume))
        logging.info("Available Volume:" + str(self.avail_volume))


class Position:
    def __init__(self, total_budget, max_hold):
        self.hold = {}
        self.spare = total_budget
        self.max_hold = max_hold

    def is_full(self):
        if len(self.hold.keys()) == self.max_hold:
            return True
        return False

    def fill_slot(self, stock_id, price, volume, avail_volume, status):
        if self.is_full():
            logging.error("Reach Max_Hold")
            return
        self.hold[stock_id] = Slot(stock_id, price, volume, avail_volume, status)

    def get_slot(self, stock_id):
        return self.hold[stock_id]

    def has_stock(self, stock_id):
        return stock_id in self.hold

    def get_empty_slot(self):
        pass

    def get_hold_count(self):
        pass

    def output_info(self):
        logging.info("现金:" + str(self.spare))
        logging.info("持仓情况：")
        count = 1
        for k, v in self.hold.items():
            logging.info("+++++++++" + k + "(" + str(count) + ")++++++++++")
            v.output_info()
            count += 1


class Trader(abc.ABC):
    """
    注意，buy和sell函数，很可能依赖回调，所以注意不要实现/使用它们的返回值
    """

    def __init__(self, total_budget, max_hold):
        self.position = Position(total_budget, max_hold)

    @abstractmethod
    def buy(self, stock_id, volume, exp_price=None, dt=None):
        pass

    @abstractmethod
    def sell(self, stock_id, exp_price=None, dt=None):
        pass

    @abstractmethod
    def get_current_price(self, stock_id, dt=None):
        pass
