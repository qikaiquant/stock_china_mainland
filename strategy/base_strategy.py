class STGContext:
    def __init__(self, sdt, edt, dbt):
        self.bt_sdt = sdt
        self.bt_edt = edt
        self.db_tool = dbt


class BaseStrategy:
    def __init__(self, ctx):
        self.ctx = ctx

    def run(self):
        pass

    def visualize(self):
        pass

    def backtest(self):
        pass
