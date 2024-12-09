from trade.trader import Trader


class Emulate_Trader(Trader):
    """
    使用掘金仿真市场API
    """

    def buy(self, position, slot, dt, stock_id, jiage=None):
        pass

    def sell(self, position, slot, dt, jiage=None):
        pass
