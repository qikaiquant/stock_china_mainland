from datetime import timedelta
from strategy.base_strategy import *
from utils.misc import *

import matplotlib.pyplot as plt
import talib
import pandas


def draw_STG(res):
    plt.figure(figsize=(10, 6), dpi=100)
    plt.plot(res['dt'], res['stg_SMA'], color='red', label='stg_SMA')
    plt.plot(res['dt'], res['bm_000300.XSHG'], color='blue', label='bm_000300.XSHG')
    plt.legend()
    plt.show()


def get_preN_tds(trade_days, cur_day, days):
    res = []
    for i in range(1, len(trade_days)):
        t = cur_day - timedelta(days=i)
        if (t,) in trade_days:
            res.append(t)
        if len(res) == days:
            break
    return res


class MaStrategy(BaseStrategy):

    def __init__(self, ctx):
        super().__init__(ctx)
        self.stock_price_map = {}
        self.stock_info_map = {}

    def calc_networth_log(self, position, dt, fp):
        log_array = [str(dt)]
        nw = position.spare
        for stock_id, (_, volumn) in position.hold.items():
            stock_name = self.stock_info_map[stock_id]
            price = self.stock_price_map[stock_id].loc[dt, 'close']
            nw += price * volumn
            log_item = stock_name + "|" + str(price) + "|" + str(volumn)
            log_array.append(log_item)
        log_array.append(str(position.spare))
        log_array.insert(1, str(nw))
        print(",".join(log_array), file=fp)
        return nw

    def pick_candidate_position(self, cur_day, trade_days, filter_list):
        pcp = []
        # 找到金叉模式候选
        for stock_id, df in self.stock_price_map.items():
            if stock_id in filter_list:
                continue
            # 需要保证这些日期都是有交易的
            if cur_day not in df.index:
                continue
            missing = False
            for td in trade_days:
                if td not in df.index:
                    missing = True
            if missing:
                break
            # End 需要保证这些日期都是有交易的
            pre1 = trade_days[0]
            if df.loc[pre1, 'SMA7'] <= df.loc[pre1, 'SMA15']:
                continue
            flag = True
            for i in range(1, len(trade_days)):
                preI = trade_days[i]
                if df.loc[preI, 'SMA7'] > df.loc[preI, 'SMA15']:
                    flag = False
                    break
            if flag:
                t = (stock_id, df.loc[cur_day, 'avg'], df.loc[cur_day, 'money'])
                # 滤除交易金额过低的票
                if t[2] > 100000:
                    pcp.append(t)
        return pcp

    def backtest(self):
        # 载入BenchMark
        self.load_benchmark(['000300.XSHG'])
        # 打开回测log文件
        fp = open('../log/SMA_Detail.csv', 'w')
        # 载入回测周期内全部股票行情并计算SMA
        stocks = self.ctx.db_tool.get_stock_info(['stock_id', 'cn_name', 'start_date', 'end_date'])
        count = 1
        for stock in stocks:
            stock_id = stock[0]
            self.stock_info_map[stock_id] = stock[1]

            start_date = datetime.datetime.strptime(self.ctx.bt_sdt, '%Y-%m-%d').date()
            end_date = datetime.datetime.strptime(self.ctx.bt_edt, '%Y-%m-%d').date()
            res = self.ctx.db_tool.get_price(stock_id, ['dt', 'close', 'avg', 'money'], start_date - timedelta(days=30),
                                             end_date)
            if len(res) == 0:
                continue
            stock_df = pandas.DataFrame(res, columns=['dt', 'close', 'avg', 'money'])
            stock_df.set_index('dt', inplace=True)

            stock_df['SMA15'] = talib.SMA(stock_df['close'], timeperiod=15)
            stock_df['SMA7'] = talib.SMA(stock_df['close'], timeperiod=7)
            stock_df['SMA15'].fillna(method='bfill', inplace=True)
            stock_df['SMA7'].fillna(method='bfill', inplace=True)

            self.stock_price_map[stock_id] = stock_df
            count += 1
            if count == 500:
                break
        # 遍历回测周期，执行回测
        # 最多建仓5支
        all_trade_days = self.ctx.db_tool.get_trade_days()
        net_worth = []
        for dt in self.ctx.bt_res['dt']:
            postion = self.ctx.ps
            # 如果发现SMA7<SMA15，以当日avg价格平仓
            keys = list(postion.hold.keys())
            for stock_id in keys:
                df = self.stock_price_map[stock_id]
                pre_1_td = get_preN_tds(all_trade_days, dt, 1)[0]
                if df.loc[pre_1_td, 'SMA7'] < df.loc[pre_1_td, 'SMA15']:
                    postion.sell(stock_id, df.loc[dt, 'avg'], sell_all=True)
            # 如果不满仓，补足
            if len(postion.hold) < 5:
                pre_N_td = get_preN_tds(all_trade_days, dt, 8)
                res = self.pick_candidate_position(dt, pre_N_td, postion.hold.keys())
                if len(res) == 0:
                    log("Pick NO Candidate")
                    net_worth.append(self.calc_networth_log(postion, dt, fp))
                    continue
                res.sort(key=lambda x: x[2], reverse=True)
                # num：剩余仓位和候选集合之间的小值
                num = 5 - len(postion.hold)
                if num > len(res):
                    num = len(res)
                # End num
                budget = float(postion.spare) / num
                for item in res:
                    if len(postion.hold) == 5:
                        break
                    postion.buy(item[0], item[1], budget)
            net_worth.append(self.calc_networth_log(postion, dt, fp))
        fp.close()
        self.ctx.bt_res.insert(loc=1, column='stg_SMA', value=net_worth)
        draw_STG(self.ctx.bt_res)
