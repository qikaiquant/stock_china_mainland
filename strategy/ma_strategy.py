from strategy.base_strategy import BaseStrategy


class MaStrategy(BaseStrategy):
    def prepare(self):
        return True

    def traversal(self, stock_list):
        return True

    def pick(self, stock_list, ps_list):
        print("I am son - ma pick")
        return True

    def backtest(self, btc):
        return True
