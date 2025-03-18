from xtquant.xttrader import XtQuantTraderCallback, XtQuantTrader
from xtquant.xttype import StockAccount

from trade.trader import Trader
from utils.common import *


class QMTTraderCallback(XtQuantTraderCallback):
    def on_disconnected(self):
        logging.info("connection lost")

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
    def __init__(self, total_budget, max_hold, real_flag):
        super().__init__(total_budget, max_hold)
        if real_flag:
            account = conf_dict["Trade"]["QMT"]["Trade_Account"]
            path = conf_dict["Trade"]["QMT"]["Trade_Path"]
        else:
            account = conf_dict["Trade"]["QMT"]["Test_Account"]
            path = conf_dict["Trade"]["QMT"]["Tes_Path"]

    def buy(self, stock_id, budget, exp_price=None, dt=None):
        pass

    def sell(self, stock_id, exp_price=None, dt=None):
        pass

    def get_current_price(self, stock_id, dt=None):
        pass


def main():
    path = "D:\\国金QMT交易端模拟\\userdata_mini"
    session_id = 123456
    xt_trader = XtQuantTrader(path, session_id)
    account = StockAccount("39136468")

    callback = QMTTraderCallback()
    xt_trader.register_callback(callback)

    xt_trader.start()

    connect_result = xt_trader.connect()
    print(connect_result)

    subscribe_result = xt_trader.subscribe(account)
    print(subscribe_result)

    asset = xt_trader.query_stock_asset(account)
    print(asset.cash, asset.total_asset)

    positions = xt_trader.query_stock_positions(account)
    for position in positions:
        print(position.stock_code, position.volume, position.open_price, position.market_value)
