import getopt
import os.path
import sys

import matplotlib.pyplot as plt
import numpy
import pandas
from dateutil import relativedelta

sys.path.append(os.path.dirname(sys.path[0]))
from strategy.base_strategy import *


def _draw_res(df, id_dict, output_dir, title):
    fig = plt.figure(figsize=(10, 6), dpi=100)
    plt.rc('font', family='FangSong', size=14)
    # 左侧折线图
    left, bottom, width, height = 0.07, 0.2, 0.7, 0.6
    ax1 = fig.add_axes([left, bottom, width, height])
    ax1.plot(df.index, df['000300.XSHG'], color='slategrey', label="基线")
    ax1.plot(df.index, df['stg_nw'], color='darkred', label="策略")
    ax1.grid(linestyle='--')
    ax1.set_facecolor('whitesmoke')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.annotate(text='', xytext=(id_dict['策略']['最大回撤'][0], df.loc[id_dict['策略']['最大回撤'][0], 'stg_nw']),
                 xy=(id_dict['策略']['最大回撤'][1], df.loc[id_dict['策略']['最大回撤'][1], 'stg_nw']),
                 arrowprops=dict(arrowstyle='->', color='r', linewidth=2))
    ax1.legend(loc='best')
    # 右侧数据指标表格
    left, bottom, width, height = 0.77, 0.2, 0.2, 0.6
    ax2 = fig.add_axes([left, bottom, width, height])

    celltext = [['收益', '{:.1%}'.format(id_dict['策略']['收益率']), '{:.1%}'.format(id_dict['基准']['收益率'])],
                ['年化收益', '{:.1%}'.format(id_dict['策略']['年化收益率']),
                 '{:.1%}'.format(id_dict['基准']['年化收益率'])],
                ['最大回撤', '{:.1%}'.format(id_dict['策略']['最大回撤'][2]), '-'],
                ['夏普比率', '{:.2f}'.format(id_dict['策略']['夏普比率']), '-']]
    columns = ['指标', "策略", "基线"]
    ax2.axis('off')
    tb = ax2.table(cellText=celltext, colLabels=columns, loc='lower left', cellLoc='center', rowLoc='bottom')
    tb.scale(1.1, 1.3)

    plt.title(title)
    plt.savefig(os.path.join(output_dir, title), dpi=600)


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
    avg_monthly_rfr = conf_dict['Backtest']['Risk_Free_Rate'] / 12
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


def _get_valuable(result):
    rr_flag = True
    if result["策略"]["收益率"] < result["基准"]["收益率"]:
        rr_flag = False
    drawback_flag = True
    delta_bt = conf_dict['Backtest']['End_Date'] - conf_dict['Backtest']['Start_Date']
    delta_db = result['策略']["最大回撤"][1] - result['策略']["最大回撤"][0]
    if delta_bt * 0.7 < delta_db:
        drawback_flag = False
    return rr_flag and drawback_flag


def _get_index(result):
    day_first = result.index[0]
    day_last = result.index[-1]
    days_delta = day_last - day_first
    index_dict = {"基准": {}, "策略": {}}
    # 基准策略，只计算收益率和年化收益率
    base_index_dict = {}
    base_rr = (result.loc[day_last, '000300.XSHG'] - result.loc[day_first, '000300.XSHG']) / result.loc[day_first, '000300.XSHG']
    base_index_dict['收益率'] = base_rr
    base_rr_year = ((result.loc[day_last, '000300.XSHG'] - result.loc[day_first, '000300.XSHG']) * 365 / days_delta.days) / \
                   result.loc[day_first, '000300.XSHG']
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


def batch_analyze():
    cachetool = RedisTool(conf_dict['Redis']['Host'], conf_dict['Redis']['Port'],
                          conf_dict['Redis']['Passwd'])
    benchmark_res = pandas.DataFrame(cachetool.get(BENCHMARK_KEY, COMMON_CACHE_ID, serialize=True))
    stg_id = conf_dict["Backtest"]['STG']
    pattern = RES_KEY_PREFIX + stg_id + ":*"
    keys = cachetool.get_keys(COMMON_CACHE_ID, pattern)
    files = os.listdir(conf_dict["Backtest"]["Analyze_Res_Dir"])
    for key in keys:
        str_key = key.decode()
        pid = str_key.split(":")[2]
        f_name = pid + ".png"
        if f_name in files:
            logging.info(f_name + " HAS been Drew, Ignore.")
            continue
        stg_res = cachetool.get(key, COMMON_CACHE_ID, serialize=True)
        if len(stg_res) != len(benchmark_res):
            logging.error("Key[" + str_key + "] Error.PLS Check.")
            continue
        res = pandas.merge(benchmark_res, stg_res, left_index=True, right_index=True)
        index = _get_index(res)
        is_valuable = _get_valuable(index)
        if is_valuable:
            output_dir = os.path.join(conf_dict["Backtest"]["Analyze_Res_Dir"], "good")
        else:
            output_dir = os.path.join(conf_dict["Backtest"]["Analyze_Res_Dir"], "bad")
        _draw_res(res, index, output_dir, pid)
        # cachetool.delete(key, COMMON_CACHE_ID)


def single_analyze():
    cachetool = RedisTool(conf_dict['Redis']['Host'], conf_dict['Redis']['Port'],
                          conf_dict['Redis']['Passwd'])
    benchmark_res = pandas.DataFrame(cachetool.get(BENCHMARK_KEY, COMMON_CACHE_ID, serialize=True))
    res_key = "RES_KEY:TJMG"
    stg_res = cachetool.get(res_key, COMMON_CACHE_ID, serialize=True)
    if len(stg_res) != len(benchmark_res):
        logging.error("Key[" + res_key + "] Error.PLS Check.")
        return
    res = pandas.merge(benchmark_res, stg_res, left_index=True, right_index=True)
    index = _get_index(res)
    _draw_res(res, index, conf_dict["Backtest"]["Analyze_Res_Dir"], "analyze")


if __name__ == '__main__':
    opts, args = getopt.getopt(sys.argv[1:], "", longopts=["batch", "single"])
    for opt, arg in opts:
        if opt == '--batch':
            batch_analyze()
        elif opt == '--single':
            single_analyze()
        else:
            logging.error("Usage Error")
            sys.exit(1)
