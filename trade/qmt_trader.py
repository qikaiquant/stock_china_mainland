import time

from xtquant.xttrader import XtQuantTraderCallback, XtQuantTrader
from xtquant.xttype import StockAccount

from trade.trader import Trader
from utils.common import *


class QMTTraderCallback(XtQuantTraderCallback):
    def on_disconnected(self):
        logging.fatal("Connection Lost")

    def on_stock_order(self, order):
        logging.info("on order callback:")
        logging.info(order.stock_code, order.order_status, order.order_sysid)

    def on_stock_trade(self, trade):
        logging.info("on trade callback")
        logging.info(trade.account_id, trade.stock_code, trade.order_id)

    def on_order_error(self, order_error):
        logging.info("on order_error callback")
        logging.info(order_error.order_id, order_error.error_id, order_error.error_msg)

    def on_cancel_error(self, cancel_error):
        logging.info("on cancel_error callback")
        logging.info(cancel_error.order_id, cancel_error.error_id, cancel_error.error_msg)

    def on_order_stock_async_response(self, response):
        logging.info("on_order_stock_async_response")
        logging.info(response.account_id, response.order_id, response.seq)

    def on_account_status(self, status):
        logging.info("on_account_status")
        logging.info(status.account_id, status.account_type, status.status)


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
        self.xt_trader.register_callback(QMTTraderCallback)
        self.xt_trader.start()
        connect_result = self.xt_trader.connect()
        subscribe_result = self.xt_trader.subscribe(self.account)
        print(connect_result, subscribe_result)
        if connect_result != 0 or subscribe_result != 0:
            logging.fatal("MUST Check.CANNOT connect/subscribe QMT")
            raise Exception("Connect/Subscribe QMT Error")
        # 载入持仓情况
        self._load_position()

    def _load_position(self):
        pass

    def buy(self, stock_id, budget, exp_price=None, dt=None):
        pass

    def sell(self, stock_id, exp_price=None, dt=None):
        pass

    def get_current_price(self, stock_id, dt=None):
        pass

    def check(self):
        asset = self.xt_trader.query_stock_asset(self.account)
        print(asset.cash, asset.total_asset)
        positions = self.xt_trader.query_stock_positions(self.account)
        for position in positions:
            print(position.stock_code, position.volume, position.open_price, position.market_value)


if __name__ == '__main__':
    t = QMTTrader(200000, 10)
