from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import StockAccount


class MyXtQuantTraderCallback(XtQuantTraderCallback):
    def on_disconnected(self):
        """
        连接断开
        :return:
        """
        print("connection lost")

    def on_stock_order(self, order):
        """
        委托回报推送
        :param order: XtOrder对象
        :return:
        """
        print("on order callback:")
        print(order.stock_code, order.order_status, order.order_sysid)

    def on_stock_trade(self, trade):
        """
        成交变动推送
        :param trade: XtTrade对象
        :return:
        """
        print("on trade callback")
        print(trade.account_id, trade.stock_code, trade.order_id)

    def on_order_error(self, order_error):
        """
        委托失败推送
        :param order_error:XtOrderError 对象
        :return:
        """
        print("on order_error callback")
        print(order_error.order_id, order_error.error_id, order_error.error_msg)

    def on_cancel_error(self, cancel_error):
        """
        撤单失败推送
        :param cancel_error: XtCancelError 对象
        :return:
        """
        print("on cancel_error callback")
        print(cancel_error.order_id, cancel_error.error_id, cancel_error.error_msg)

    def on_order_stock_async_response(self, response):
        """
        异步下单回报推送
        :param response: XtOrderResponse 对象
        :return:
        """
        print("on_order_stock_async_response")
        print(response.account_id, response.order_id, response.seq)

    def on_account_status(self, status):
        """
        :param response: XtAccountStatus 对象
        :return:
        """
        print("on_account_status")
        print(status.account_id, status.account_type, status.status)


def main():
    path = "D:\\国金QMT交易端模拟\\userdata_mini"
    session_id = 123456
    xt_trader = XtQuantTrader(path, session_id)
    account = StockAccount("39136468")

    callback = MyXtQuantTraderCallback()
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

    # xt_trader.run_forever()


if __name__ == "__main__":
    main()
