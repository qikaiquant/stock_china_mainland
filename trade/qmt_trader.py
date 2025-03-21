import time

from xtquant import xtdata
from xtquant.xttrader import XtQuantTraderCallback, XtQuantTrader
from xtquant.xttype import StockAccount

from trade.trader import Trader, SlotStatus
from utils.common import *


class QMTTraderCallback(XtQuantTraderCallback):
    def on_disconnected(self):
        print("Connection Lost")

    def on_stock_order(self, order):
        print("on order callback:")
        print(order.stock_code, order.order_status, order.order_sysid)

    def on_stock_trade(self, trade):
        print("on trade callback")
        print(trade.account_id, trade.stock_code, trade.order_id)

    def on_order_error(self, order_error):
        print("on order_error callback")
        print(order_error.order_id, order_error.error_id, order_error.error_msg)

    def on_cancel_error(self, cancel_error):
        print("on cancel_error callback")
        print(cancel_error.order_id, cancel_error.error_id, cancel_error.error_msg)

    def on_order_stock_async_response(self, response):
        print("on_order_stock_async_response")
        print(response.account_id, response.order_id, response.seq)

    def on_account_status(self, status):
        print("on_account_status")
        print(status.account_id, status.account_type, status.status)


class QMTTrader(Trader):
    def __init__(self, total_budget, max_hold, real_flag=False):
        super().__init__(total_budget, max_hold)
        # 初始化qmt连接
        if real_flag:
            account_str = conf_dict["Trade"]["QMT"]["Trade_Account"]
            path = conf_dict["Trade"]["QMT"]["Trade_Path"]
        else:
            account_str = conf_dict["Trade"]["QMT"]["Test_Account"]
            path = conf_dict["Trade"]["QMT"]["Tes_Path"]
        session_id = int(time.time())
        self.account = StockAccount(account_str)
        self.xt_trader = XtQuantTrader(path, session_id)
        cb = QMTTraderCallback()
        self.xt_trader.register_callback(cb)
        self.xt_trader.start()
        connect_result = self.xt_trader.connect()
        subscribe_result = self.xt_trader.subscribe(self.account)
        if connect_result != 0 or subscribe_result != 0:
            logging.fatal("MUST Check.CANNOT connect/subscribe QMT")
            raise Exception("Connect/Subscribe QMT Error")
        # 载入持仓情况
        self._load_position()

    def _load_position(self):
        asset = self.xt_trader.query_stock_asset(self.account)
        self.position.spare = asset.cash
        positions = self.xt_trader.query_stock_positions(self.account)
        for p in positions:
            self.position.fill_slot(p.stock_code, p.avg_price, p.volume, p.can_use_volume, SlotStatus.Keep)

    def buy(self, stock_id, volume, exp_price=None, dt=None):
        position = self.position
        if position.is_full() and not position.has_stock(stock_id):
            logging.error("Hold is Full, CANNOT Buy.")
            return
        if exp_price is None:
            # TODO 市价
            pass
        else:
            # TODO 限价
            pass
        # self.xt_trader.order_stock_async(self.account, "000701.SZ", xtconstant.STOCK_BUY, 300, xtconstant.LATEST_PRICE,
        #                                 -1, "stg1", 'order2')

    def sell(self, stock_id, exp_price=None, dt=None):
        data = xtdata.get_full_tick(["002614.SZ"])
        print(data)

    def get_current_price(self, stock_id, dt=None):
        pass


if __name__ == '__main__':
    t = QMTTrader(200000, 10)
    t.buy("000541.SZ", 7000)
