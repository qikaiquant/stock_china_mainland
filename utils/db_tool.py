import pymysql as pms
import traceback as tb
import pandas as pd


class DBTool:
    def __init__(self, host, port, user, passwd):
        print("Start Init")
        self._conn = pms.connect(host=host, port=port, user=user, passwd=passwd)
        self._cursor = self._conn.cursor()

    def clear_table(self, table_name):
        sql = "truncate table " + table_name
        self._cursor.execute(sql)
        self._conn.commit()

    def execute_raw_sql(self, sqls, is_commit):
        for sql in sqls:
            self._cursor.execute(sql)
        if is_commit:
            self._conn.commit()

    def insert_trade_days(self, ds):
        # 先清空再插入，只支持全量操作
        self.clear_table("quant_stock.stock_trade_days")
        for day in ds:
            sql = "insert ignore into quant_stock.stock_trade_days values(\'" + str(day) + "\')"
            self._cursor.execute(sql)
        self._conn.commit()

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
                print('Commit')
                self._conn.commit()
                commit_count = 0
        self._conn.commit()

    def __del__(self):
        self._cursor.close()
        self._conn.close()
        print("DB Connection Closed.")
