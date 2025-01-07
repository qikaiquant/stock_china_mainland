import getopt
import os
import random
import sys
import time

import pandas
import xlrd

sys.path.append(os.path.dirname(sys.path[0]))

from jqdatasdk import *
from utils.db_tool import *
from utils.common import *

JK_User = None
JK_Token = None

Stock_DB_Tool = None


def check_spare():
    auth(JK_User, JK_Token)
    logging.info(get_query_count())
    logout()


def fetch_trade_days():
    auth(JK_User, JK_Token)
    Stock_DB_Tool.refresh_trade_days(get_all_trade_days())
    logout()


def fetch_sw_class():
    f = xlrd.open_workbook(conf_dict["DataSource"]["SW_Industry_Code_File"])
    t = f.sheets()[0]
    res = []
    for row in t:
        if row[0].value == '行业代码':
            continue
        cid = int(row[0].value)
        indus_level_1 = row[1].value
        if indus_level_1 == "":
            indus_level_1 = 'NA'
        indus_level_2 = row[2].value
        if indus_level_2 == "":
            indus_level_2 = 'NA'
        indus_level_3 = row[3].value
        if indus_level_3 == "":
            indus_level_3 = 'NA'
        detail = indus_level_1 + "|" + indus_level_2 + "|" + indus_level_3
        res.append((cid, detail))
    Stock_DB_Tool.refresh_sw_industry_code(res)


def _attach_ext_info(stock_info):
    # load 1：sw分类信息
    f = xlrd.open_workbook(conf_dict["DataSource"]["SW_Stock_Code_File"])
    t = f.sheets()[0]
    stock_industry_map = {}
    for row in t:
        stock_id = row[0].value
        industry_id = row[2].value
        if stock_id not in stock_industry_map:
            stock_industry_map[stock_id] = [industry_id]
        else:
            stock_industry_map[stock_id].append(industry_id)
    # attach过程
    for stock_id, stock_info_item in stock_info.items():
        ext_dict = stock_info_item['ext']
        # attach申万分类
        pure_id = stock_id.split('.')[0]
        if pure_id in stock_industry_map:
            ext_dict["SW_Code"] = stock_industry_map[pure_id]


def fetch_all_stock_info():
    logging.info("Start Get All Stock Info")
    auth(JK_User, JK_Token)
    stock_info = {}
    for index, row in pandas.DataFrame(get_all_securities(['stock'])).iterrows():
        stock_info[index] = {}
        stock_info[index]['display_name'] = row['display_name']
        stock_info[index]['name'] = row['name']
        stock_info[index]['start_date'] = row['start_date']
        stock_info[index]['end_date'] = row['end_date']
        stock_info[index]['ext'] = {}
    _attach_ext_info(stock_info)
    Stock_DB_Tool.refresh_stock_info(stock_info)
    logout()
    logging.info("End Get All Stock Info")


def fetch_indicator():
    logging.info("Start Get Indicator")
    auth(JK_User, JK_Token)
    stocks = Stock_DB_Tool.get_stock_info(['stock_id', 'start_date', 'end_date'])
    for (stock_id, ipo_date, delist_date) in stocks:
        # 获取已在库的quarter数据
        fetched_quarter = set()
        res = Stock_DB_Tool.get_indicator(stock_id, ['sid', 'quarter'])
        for (_, q) in res:
            fetched_quarter.add(q)
        # 获取待抓取的quarter数据
        start_date = datetime.strptime('2013-01-01', '%Y-%m-%d').date()
        end_date = datetime.today().date()
        # 退市时间在2013年1月1日之前，不参考
        if delist_date < start_date:
            continue
        # 已退市股票，以退市时间为准
        if delist_date < end_date:
            end_date = delist_date
        # 上市时间晚于2013年1月1日，以上市时间为准
        if ipo_date > start_date:
            start_date = ipo_date
        # 获取上市期间的季度数
        tbf_quarter = []
        if start_date.year == end_date.year:
            for j in range(get_quarter(start_date), get_quarter(end_date)):
                tbf_quarter.append((start_date.year, j))
        else:
            for i in range(start_date.year, end_date.year + 1):
                if i == start_date.year:
                    q = get_quarter(start_date)
                    for j in range(q, 5):
                        tbf_quarter.append((i, j))
                elif i == end_date.year:
                    q = get_quarter(end_date)
                    for j in range(1, q):
                        tbf_quarter.append((i, j))
                else:
                    for j in range(1, 5):
                        tbf_quarter.append((i, j))
        # 抓取不在库的数据
        for (year, quarter) in tbf_quarter:
            yq = 10 * year + quarter
            logging.info(stock_id + ":" + str(yq))
            if yq in fetched_quarter:
                continue
            q = query(indicator).filter(indicator.code == stock_id)
            df = get_fundamentals(q, statDate=str(year) + "q" + str(quarter))
            Stock_DB_Tool.insert_indicator(stock_id, df, yq)
    logout()
    logging.info("End Get Indicator")


def fetch_daily():
    logging.info("Start Fetch Daily")
    auth(JK_User, JK_Token)
    # 抓取行情、估值和st
    tfb_list = Stock_DB_Tool.get_tbf()
    for (stock_id, start_date, end_data, tbf_type) in tfb_list:
        logging.debug("Will fetch " + stock_id + ". Spare is " + str(get_query_count()))
        if tbf_type == ToBeFetchType.PRICE.value:
            pi = get_price(stock_id, end_date=end_data, start_date=start_date, frequency='daily',
                           fields=['open', 'close', 'low', 'high', 'volume', 'money', 'factor', 'high_limit',
                                   'low_limit', 'avg', 'pre_close', 'paused'])
            Stock_DB_Tool.insert_price(stock_id, pandas.DataFrame(pi))
        elif tbf_type == ToBeFetchType.VALUATION.value:
            pi = get_valuation(stock_id, start_date=start_date, end_date=end_data, fields=[])
            Stock_DB_Tool.insert_valuation(stock_id, pandas.DataFrame(pi))
        elif tbf_type == ToBeFetchType.ST.value:
            pi = get_extras('is_st', [stock_id], start_date=start_date, end_date=end_data)
            Stock_DB_Tool.insert_st(stock_id, pandas.DataFrame(pi))
        Stock_DB_Tool.remove_tbf((stock_id, start_date, end_data, tbf_type))
        time.sleep(0.1)
    logout()
    logging.info("End Fetch Daily")


def _check_fq(stock_id, fetched_dt):
    """
    因为JK的数据是前复权，所以有可能某支股票的价格会变动。
    该函数探测这个变动，并返回一个布尔值，决定是否重抓数据
    """
    if len(fetched_dt) == 0:
        return True
    check_points = []
    if len(fetched_dt) < 4:
        check_points.extend(fetched_dt)
    else:
        check_points = [fetched_dt[0]]
        check_points.extend(random.sample(fetched_dt, 3))
    need_refresh = False
    for dt in check_points:
        db_res = Stock_DB_Tool.get_price(stock_id, ['close'], dt, dt)
        jk_res = get_price(stock_id, end_date=dt, count=1, frequency='daily', fields=['close'])
        db_close = db_res[0][0]
        jk_close = jk_res.loc[str(dt), "close"]
        if db_close != jk_close:
            logging.info(
                stock_id + "[" + str(dt) + "]Close Price DIFF:[DB]" + str(db_close) + " vs. [JK]" + str(jk_close))
            need_refresh = True
    return need_refresh


def scan_price():
    logging.info("Start Scan Price")
    auth(JK_User, JK_Token)
    stocks = Stock_DB_Tool.get_stock_info(['stock_id', 'start_date', 'end_date'])
    for (stock_id, ipo_date, delist_date) in stocks:
        start_date = datetime.strptime('2013-01-01', '%Y-%m-%d').date()
        end_date = datetime.today().date()
        # 退市时间在2013年1月1日之前，不参考
        if delist_date < start_date:
            continue
        # 已退市股票，以退市时间为准
        if delist_date < end_date:
            end_date = delist_date
        # 上市时间晚于2013年1月1日，以上市时间为准
        if ipo_date > start_date:
            start_date = ipo_date
        all_dt = Stock_DB_Tool.get_trade_days(start_date, end_date)
        fetched_dt = []
        for (dt,) in Stock_DB_Tool.get_price(stock_id, ['dt'], start_date, end_date):
            fetched_dt.append(dt)
        # 如果股票的复权信息发生变化，需要全部更新
        if _check_fq(stock_id, fetched_dt):
            Stock_DB_Tool.insert_tbf(stock_id, ToBeFetchType.PRICE, all_dt)
            logging.info(stock_id + " FQ info Changed, Will Refresh ALL price For it.")
            continue
        # 否则只部分更新：将tbf整理成分段的日期并写库
        logging.debug(stock_id + " Will Refresh Partly.")
        tbf_dt = []
        for dt in all_dt:
            if dt not in fetched_dt:
                tbf_dt.append(dt)
                continue
            Stock_DB_Tool.insert_tbf(stock_id, ToBeFetchType.PRICE, tbf_dt)
            tbf_dt = []
        Stock_DB_Tool.insert_tbf(stock_id, ToBeFetchType.PRICE, tbf_dt)
    logout()
    logging.info("End Scan Price")


def scan_valuation():
    logging.info("Start Scan Valuation")
    stocks = Stock_DB_Tool.get_stock_info(['stock_id', 'start_date', 'end_date'])
    for (stock_id, ipo_date, delist_date) in stocks:
        start_date = datetime.strptime('2013-01-01', '%Y-%m-%d').date()
        end_date = datetime.today().date()
        # 退市时间在2013年1月1日之前，不参考
        if delist_date < start_date:
            continue
        # 已退市股票，以退市时间为准
        if delist_date < end_date:
            end_date = delist_date
        # 上市时间晚于2013年1月1日，以上市时间为准
        if ipo_date > start_date:
            start_date = ipo_date
        all_dt = Stock_DB_Tool.get_trade_days(start_date, end_date)
        fetched_dt = []
        for (dt, mc) in Stock_DB_Tool.get_valuation_st(stock_id, ['dt', 'market_cap'], start_date, end_date):
            if mc is None:
                continue
            fetched_dt.append(dt)
        # 合并分段
        tbf_dt = []
        for dt in all_dt:
            if dt not in fetched_dt:
                tbf_dt.append(dt)
                continue
            Stock_DB_Tool.insert_tbf(stock_id, ToBeFetchType.VALUATION, tbf_dt)
            tbf_dt = []
        Stock_DB_Tool.insert_tbf(stock_id, ToBeFetchType.VALUATION, tbf_dt)
    logging.info("End Scan Valuation")


def scan_st():
    logging.info("Start Scan ST")
    stocks = Stock_DB_Tool.get_stock_info(['stock_id', 'start_date', 'end_date'])
    for (stock_id, ipo_date, delist_date) in stocks:
        start_date = datetime.strptime('2013-01-01', '%Y-%m-%d').date()
        end_date = datetime.today().date()
        # 退市时间在2013年1月1日之前，不参考
        if delist_date < start_date:
            continue
        # 已退市股票，以退市时间为准
        if delist_date < end_date:
            end_date = delist_date
        # 上市时间晚于2013年1月1日，以上市时间为准
        if ipo_date > start_date:
            start_date = ipo_date
        all_dt = Stock_DB_Tool.get_trade_days(start_date, end_date)
        fetched_dt = []
        for (dt, st) in Stock_DB_Tool.get_valuation_st(stock_id, ['dt', "st"], start_date, end_date):
            if st is None:
                continue
            fetched_dt.append(dt)
        # 合并分段
        tbf_dt = []
        for dt in all_dt:
            if dt not in fetched_dt:
                tbf_dt.append(dt)
                continue
            Stock_DB_Tool.insert_tbf(stock_id, ToBeFetchType.ST, tbf_dt)
            tbf_dt = []
        Stock_DB_Tool.insert_tbf(stock_id, ToBeFetchType.ST, tbf_dt)
    logging.info("End Scan ST")


if __name__ == '__main__':
    # 初始化数据库
    Stock_DB_Tool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                           conf_dict['Mysql']['Passwd'])
    # 聚宽账号
    JK_User = conf_dict['DataSource']['JK_User']
    JK_Token = conf_dict['DataSource']['JK_Token']
    # 行情抓取相关配置
    opts, args = getopt.getopt(sys.argv[1:], "",
                               longopts=["spare", "trade_days", "sw_class", "all_stock_info", "scan_daily",
                                         "fetch_daily"])
    for opt, _ in opts:
        if opt == '--spare':
            check_spare()
        elif opt == '--trade_days':
            # 获取所有交易日，基本不用跑
            fetch_trade_days()
        elif opt == '--sw_class':
            # 获取申万分类，基本不用跑
            fetch_sw_class()
        elif opt == '--all_stock_info':
            # 获取所有股票的基本信息
            fetch_all_stock_info()
        elif opt == '--scan_daily':
            # 获取待抓取的列表
            Stock_DB_Tool.clear_tbf()
            # 扫行情，单独拎出来是因为将来可能改造成分钟级
            scan_price()
            # 扫估值和st,天级
            scan_valuation()
            scan_st()
        elif opt == '--fetch_daily':
            # 抓取行情、估值和ST
            fetch_daily()
            # 抓取财务指标
            fetch_indicator()
        else:
            logging.error("Usage Error")
            sys.exit(1)
