import pymysql as pms

from utils.common import *
from datetime import datetime


class DBTool:
    def __init__(self, host, port, user, passwd):
        logging.info("DB Init")
        self._conn = pms.connect(host=host, port=port, user=user, passwd=passwd)
        self._cursor = self._conn.cursor()

    def clear_table(self, table_name):
        sql = "truncate table " + table_name
        self._cursor.execute(sql)
        self._conn.commit()

    def exec_raw_select(self, sql):
        self._cursor.execute(sql)
        return self._cursor.fetchall()

    def get_price(self, stock_id, fields, start_dt=None, end_dt=None):
        fields_str = "*"
        if len(fields) != 0:
            fields_str = ",".join(fields)
        suffix = stockid2table(stock_id)
        table_name = "quant_stock.price_daily_r" + str(suffix)
        if not start_dt:
            start_dt = datetime.strptime('2013-01-01', '%Y-%m-%d')
        if not end_dt:
            end_dt = datetime.today()
        sql = "select " + fields_str + " from " + table_name + " where sid = \'" + stock_id + "\' and dt >= \'" + str(
            start_dt) + "\' and dt <= \'" + str(end_dt) + "\' order by dt"
        self._cursor.execute(sql)
        res = self._cursor.fetchall()
        return res

    def insert_price(self, stock_id, prices):
        suffix = stockid2table(stock_id)
        table_name = "quant_stock.price_daily_r" + str(suffix)
        commit_count = 0
        for index, row in prices.iterrows():
            dt = str(index).split()[0]
            paused = 0
            try:
                paused = int(float(row["paused"]))
            except ValueError:
                # 发生这种情况，很可能是这支股票已退市，但数据库里尚未更新
                print(index, row)
                continue
            sql = 'insert ignore into ' + table_name + " values(\'" + stock_id + "\',\'" + dt + "\'," + str(
                row["open"]) + "," + str(row["close"]) + "," + str(row["low"]) + "," + str(row["high"]) + "," + str(
                row["volume"]) + "," + str(row["money"]) + "," + str(row["factor"]) + "," + str(
                row["high_limit"]) + "," + str(row["low_limit"]) + "," + str(row["avg"]) + "," + str(
                row["pre_close"]) + "," + str(paused) + ")"
            self._cursor.execute(sql)
            commit_count += 1
            if commit_count == 100:
                self._conn.commit()
                commit_count = 0
        self._conn.commit()

    def insert_trade_days(self, ds):
        # 先清空再插入，只支持全量操作
        self.clear_table("quant_stock.stock_trade_days")
        for day in ds:
            sql = "insert ignore into quant_stock.stock_trade_days values(\'" + str(day) + "\')"
            self._cursor.execute(sql)
        self._conn.commit()

    def get_trade_days(self, start_date=None, end_date=None):
        if not start_date:
            start_date = datetime.strptime('2013-01-01', '%Y-%m-%d')
        if not end_date:
            end_date = datetime.today()
        sql = "select * from quant_stock.stock_trade_days where trade_date >= \'" + str(
            start_date) + "\' and trade_date <= \'" + str(end_date) + "\'"
        self._cursor.execute(sql)
        res = self._cursor.fetchall()
        return res

    def insert_stock_info(self, all_stock_info):
        # 先清空再插入，只支持全量操作
        self.clear_table('quant_stock.stock_info')
        commit_count = 0
        for index, row in all_stock_info.iterrows():
            sql = 'insert into quant_stock.stock_info values(\'' + str(index) + '\',\'' + row[
                'display_name'] + '\',\'' + row['name'] + '\',\'' + str(
                row['start_date']) + '\',\'' + str(row['end_date']) + '\',\'{}\')'
            self._cursor.execute(sql)
            commit_count += 1
            if commit_count == 100:
                self._conn.commit()
                commit_count = 0
        self._conn.commit()
        # 手动插入指数信息:沪深300
        sql = "insert into quant_stock.stock_info values(\'000300.XSHG\', \'沪深300\', \'HS300\'," \
              " \'2005-04-08\', \'2200-01-01\',\'{}\')"
        self._cursor.execute(sql)
        self._conn.commit()

    def get_stock_info(self, fileds):
        if fileds is None:
            print("Fields is NECESSARY.")
            return
        sql = "select " + ','.join(fileds) + " from quant_stock.stock_info"
        self._cursor.execute(sql)
        res = self._cursor.fetchall()
        return res

    def __del__(self):
        self._cursor.close()
        self._conn.close()
