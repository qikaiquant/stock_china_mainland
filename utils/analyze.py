import matplotlib.pyplot as plt
import numpy

from redis_tool import *
from strategy.base_strategy import *
from dateutil import relativedelta


def _draw_nw(df):
    plt.figure(figsize=(10, 6), dpi=100)
    plt.plot(df.index, df['stg_nw'], color='red', label='stg')
    plt.plot(df.index, df['HS300'], color='blue', label='HS300')
    plt.legend()
    # plt.savefig("D:\\test\\backtest\\macd_nw.jpg", dpi=600)
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


def _get_max_loss(df, start_dt, end_dt):
    loss_list = []
    loss_dict = {}
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


def _get_sharp_ratio(df, col):
    # 按自然月切分，找到切分点
    seg_points = []
    d_next_start = df.index[0]
    while True:
        if d_next_start > df.index[-1]:
            break
        if d_next_start in df.index:
            seg_points.append(d_next_start)
        else:
            for i in range(1, 32):
                if (d_next_start - timedelta(days=i)) in df.index:
                    seg_points.append(d_next_start - timedelta(days=i))
                    break
                if (d_next_start - timedelta(days=-i)) in df.index:
                    seg_points.append(d_next_start - timedelta(days=-i))
                    break
        d_next_start += relativedelta.relativedelta(months=1)
    # 处理尾部日期
    if df.index[-1] - seg_points[-1] < timedelta(days=10):
        seg_points.pop()
    seg_points.append(df.index[-1])
    # 下面是计算sharp值
    seg_num = len(seg_points) - 1  # 期数
    # 平均月收益率
    avg_monthly_rr = ((df.loc[seg_points[-1], col] - df.loc[seg_points[0], col]) / df.loc[seg_points[0], col]) / seg_num
    # 平均月无风险收益率
    avg_monthly_rfr = conf_dict['Backtest']['Risk_Free_Rate'] / seg_num
    # 计算标准差
    monthly_rr = []
    for i in range(0, seg_num):
        rr = (df.loc[seg_points[i + 1], col] - df.loc[seg_points[i], col]) / df.loc[seg_points[i], col]
        monthly_rr.append(rr)
    monthly_std = numpy.std(monthly_rr)
    # 夏普比率
    sharp_ratio = (avg_monthly_rr - avg_monthly_rfr) / monthly_std
    return sharp_ratio


def _get_max_drawdown(df, col):
    # 按照净值排序
    raw_list = []
    for d in df.index:
        nw = df.loc[d, col]
        raw_list.append((d, nw))
    raw_list.sort(key=lambda x: x[1], reverse=True)
    # 找到最大回撤
    max_drawdown_rate = 0.001
    max_drawdown_range = []
    for i in range(0, len(raw_list)):
        for j in range(len(raw_list) - 1, i, -1):
            if raw_list[j][0] > raw_list[i][0]:
                drawdown_rate = (raw_list[i][1] - raw_list[j][1]) / raw_list[i][1]
                if drawdown_rate > max_drawdown_rate:
                    max_drawdown_rate = drawdown_rate
                    max_drawdown_range = [raw_list[i][0], raw_list[j][0]]
                break
    # 返回最大回撤信息
    max_drawdown_range.append(max_drawdown_rate)
    return max_drawdown_range


def _get_index(result):
    day_first = result.index[0]
    day_last = result.index[-1]
    days_delta = day_last - day_first
    index_dict = {"基准": {}, "策略": {}}
    # 基准策略，只计算收益率和年化收益率
    base_index_dict = {}
    base_rr = (result.loc[day_last, 'HS300'] - result.loc[day_first, 'HS300']) / result.loc[day_first, 'HS300']
    base_index_dict['收益率'] = base_rr
    base_rr_year = ((result.loc[day_last, 'HS300'] - result.loc[day_first, 'HS300']) * 365 / days_delta.days) / \
                   result.loc[day_first, 'HS300']
    base_index_dict['年化收益率'] = base_rr_year
    index_dict['基准'] = base_index_dict
    # 测试策略，计算收益率、年化收益率、最大回撤和夏普比率
    stg_index_dict = {}
    # 收益率
    stg_rr = (result.loc[day_last, 'stg_nw'] - result.loc[day_first, 'stg_nw']) / result.loc[day_first, 'stg_nw']
    stg_index_dict['收益率'] = stg_rr
    stg_rr_year = ((result.loc[day_last, 'stg_nw'] - result.loc[day_first, 'stg_nw']) * 365 / days_delta.days) / \
                  result.loc[day_first, 'stg_nw']
    stg_index_dict['年化收益率'] = stg_rr_year
    # 最大回撤
    stg_index_dict['最大回撤'] = _get_max_drawdown(result, 'stg_nw')
    # 夏普比率
    stg_index_dict['夏普比率'] = _get_sharp_ratio(result, 'stg_nw')
    index_dict['策略'] = stg_index_dict
    return index_dict


if __name__ == '__main__':
    cachetool = RedisTool(conf_dict['Redis']['Host'], conf_dict['Redis']['Port'],
                          conf_dict['Redis']['Passwd'])
    stg_res = cachetool.get(RES_KEY, COMMON_CACHE_ID, serialize=True)
    benchmark_res = pandas.DataFrame(cachetool.get(BENCHMARK_KEY, COMMON_CACHE_ID, serialize=True))
    if len(stg_res) != len(benchmark_res):
        print("EEEERROR!!!")
        sys.exit(1)
    res = pandas.merge(benchmark_res, stg_res, left_index=True, right_index=True)
    index = _get_index(res)
    print(index)
    # _get_max_loss(res, conf_dict['Backtest']['Start_Date'], conf_dict['Backtest']['End_Date'])
    # _draw_nw(res)
    # _parse_stg_detail(res)
