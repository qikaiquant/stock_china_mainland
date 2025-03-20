import abc
from abc import abstractmethod
from enum import auto

from utils.common import *


class SlotStatus(Enum):
    Keep = auto,
    Buying = auto,
    Selling = auto


class Slot:
    def __init__(self, price, volume, status):
        self.price = price
        self.volume = volume
        self.status = status


class Position:
    def __init__(self, total_budget, max_hold):
        self.hold = {}
        self.spare = total_budget
        self.max_hold = max_hold

    def is_slot_full(self):
        if len(self.hold.keys()) == self.max_hold:
            return True
        return False

    def fill_slot(self, stock_id, price, volume, status):
        if self.is_slot_full():
            logging.error("Reach Max_Hold")
            return
        self.hold[stock_id] = Slot(price, volume, status)

    def get_slot(self, stock_id):
        return self.hold[stock_id]

    def get_empty_slot(self):
        pass

    def get_hold_count(self):
        pass


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

    @abstractmethod
    def get_current_price(self, stock_id, dt=None):
        pass
