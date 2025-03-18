import abc
from abc import abstractmethod


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

    def get_hold_count(self):
        count = 0
        for slot in self.hold:
            if slot[0] is not None:
                count += 1
        return count


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


