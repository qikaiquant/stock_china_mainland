import pymysql as pms
import utils.misc


class DBTool:
    def __init__(self, host, port, user, passwd):
        print("DB Init")
        self._conn = pms.connect(host=host, port=port, user=user, passwd=passwd)
        self._cursor = self._conn.cursor()

    def clear_table(self, table_name):
        sql = "truncate table " + table_name
        self._cursor.execute(sql)
        self._conn.commit()

    def exec_raw_select(self, sql):
        self._cursor.execute(sql)
        return self._cursor.fetchall()

    def insert_price(self, stock_id, prices):
        suffix = utils.misc.stockid2table(stock_id)
        table_name = "quant_stock.price_daily_r" + str(suffix)
        commit_count = 0
        for index, row in prices.iterrows():
            dt = str(index).split()[0]
            paused = int(float(row["paused"]))
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
        sql = "select * from quant_stock.stock_trade_days where trade_date >= \'" + start_date + "\' and trade_date <= \'" + end_date + "\'"
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
                print('Commit')
                self._conn.commit()
                commit_count = 0
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
        print("DB Connection Closed.")
