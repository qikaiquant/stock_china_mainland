import sys
import time

from common import *
from redis_tool import *
import matplotlib.pyplot as plt

from strategy.base_strategy import *


def _draw_nw(df):
    plt.figure(figsize=(10, 6), dpi=100)
    plt.plot(df.index, df['stg_nw'], color='red', label='stg')
    plt.plot(df.index, df['HS300'], color='blue', label='HS300')
    plt.legend()
    # fn = "D:\\test\\backtest\\macd_nw_" + str(time.time()) + ".jpg"
    # plt.savefig(fn, dpi=600)
    plt.show()


def _parse_stg_detail(df):
    fp = open("D:\\test\\backtest\\details.txt", 'w')
    for _, row in df.iterrows():
        print(str(row['dt']) + '\t' + str(row['stg_networth']), file=fp)
        print("\tAction : ", file=fp)
        if len(row['details']['Buy']) != 0:
            print("\t\tBuy : ", file=fp)
            for item in row['details']['Buy']:
                print("\t\t\t" + str(item), file=fp)
        if len(row['details']['Sell']) != 0:
            print("\t\tSell : ", file=fp)
            for item in row['details']['Sell']:
                print("\t\t\t" + str(item), file=fp)

        print("\tHold : ", file=fp)
        print("\t\tSpare - " + str(row['details']['Spare']), file=fp)
        if len(row['details']['Hold']) != 0:
            print("\t\tHold : ", file=fp)
            for item in row['details']['Hold']:
                print("\t\t\t" + str(item), file=fp)
    fp.close()


def _get_max_loss(df):
    loss_list = []
    loss_dict = {}
    start_dt = datetime.strptime("2022-07-20", '%Y-%m-%d').date()
    end_dt = datetime.strptime("2022-10-29", '%Y-%m-%d').date()
    df.set_index('dt', inplace=True)
    seg = df.loc[start_dt:end_dt]
    for index, row in seg.iterrows():
        dt = index
        detail = row['details']
        for item in detail['Hold']:
            stock_id = item[0]
            if stock_id not in loss_dict:
                loss_dict[stock_id] = (item[1], item[2], item[3], dt)
        for item in detail['Sell']:
            stock_id = item[0]
            jiage = item[1]
            if stock_id not in loss_dict:
                print("FFFFuck!")
            (b_jiage, b_volumn, b_total, b_dt) = loss_dict[stock_id]
            loss = (jiage - b_jiage) * b_volumn
            loss_list.append((stock_id, loss, b_volumn, (str(b_dt), b_jiage), (str(dt), jiage)))
            del loss_dict[stock_id]
    loss_list.sort(key=lambda x: x[1], reverse=False)
    for item in loss_list:
        print(item)


if __name__ == '__main__':
    cachetool = RedisTool(conf_dict['Redis']['Host'], conf_dict['Redis']['Port'],
                          conf_dict['Redis']['Passwd'])
    stg_res = cachetool.get(RES_KEY, 0, serialize=True)
    benchmark_res = pandas.DataFrame(cachetool.get(BENCHMARK_KEY, 0, serialize=True))
    if len(stg_res) != len(benchmark_res):
        print("EEEERROR!!!")
        sys.exit(1)
    res = pandas.merge(benchmark_res, stg_res, left_index=True, right_index=True)
    print(res)
    # _get_max_loss(res)
    _draw_nw(res)
    # _parse_stg_detail(res)
