import getopt
import os
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


def get_trade_days():
    auth(JK_User, JK_Token)
    Stock_DB_Tool.refresh_trade_days(get_all_trade_days())
    logout()


def get_sw_class():
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


def get_all_stock_info():
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


def scan():
    logging.info("Start Scan")
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
        # 获取未被抓取的日期列表
        all_dt = Stock_DB_Tool.get_trade_days(start_date, end_date)
        fetched_dt = []
        for (dt,) in Stock_DB_Tool.get_price(stock_id, ['dt'], start_date, end_date):
            fetched_dt.append(dt)
        # 将tbf整理成分段的日期并写库
        tbf_dt = []
        for dt in all_dt:
            if dt not in fetched_dt:
                tbf_dt.append(dt)
                continue
            Stock_DB_Tool.insert_tbf(stock_id, tbf_dt)
            tbf_dt = []
        Stock_DB_Tool.insert_tbf(stock_id, tbf_dt)
    logging.info("End Scan")


def fetch_price():
    logging.info("Start Fetch Price")
    # 获取待抓取列表
    tfb_list = Stock_DB_Tool.get_tbf()
    # 抓取并入库、修改状态
    auth(JK_User, JK_Token)
    for (stock_id, start_date, end_data) in tfb_list:
        pi = get_price(stock_id, end_date=end_data, start_date=start_date, frequency='daily',
                       fields=['open', 'close', 'low', 'high', 'volume', 'money', 'factor', 'high_limit',
                               'low_limit', 'avg', 'pre_close', 'paused'])
        Stock_DB_Tool.insert_price(stock_id, pandas.DataFrame(pi))
        Stock_DB_Tool.remove_tbf((stock_id, start_date, end_data))
        time.sleep(0.1)
    logout()
    logging.info("End Fetch Price")


if __name__ == '__main__':
    # 初始化数据库
    Stock_DB_Tool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                           conf_dict['Mysql']['Passwd'])
    # 聚宽账号
    JK_User = conf_dict['DataSource']['JK_User']
    JK_Token = conf_dict['DataSource']['JK_Token']
    # 行情抓取相关配置
    opts, args = getopt.getopt(sys.argv[1:], "",
                               longopts=["spare", "trade_days", "sw_class", "all_stock_info", "scan", "price"])
    for opt, _ in opts:
        if opt == '--spare':
            check_spare()
        elif opt == '--trade_days':
            # 获取所有交易日，基本不用跑
            get_trade_days()
        elif opt == '--sw_class':
            # 获取申万分类，基本不用跑
            get_sw_class()
        elif opt == '--all_stock_info':
            # 获取所有股票的基本信息
            get_all_stock_info()
        elif opt == '--scan':
            # 获取待抓取的列表
            scan()
        elif opt == '--price':
            # 抓取行情
            fetch_price()
        else:
            logging.error("Usage Error")
            sys.exit(1)
