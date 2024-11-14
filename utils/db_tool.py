import pymysql as pms

from utils.common import *


class DBTool:
    def __init__(self, host, port, user, passwd):
        logging.info("DB Init")
        self._conn = pms.connect(host=host, port=port, user=user, passwd=passwd)
        self._cursor = self._conn.cursor()

    def _clear_table(self, table_name):
        sql = "truncate table " + table_name
        self._cursor.execute(sql)
        self._conn.commit()

    def get_price(self, stock_id, fields, start_dt=None, end_dt=None):
        fields_str = "*"
        if len(fields) != 0:
            fields_str = ",".join(fields)
        suffix = stockid2table(stock_id)
        table_name = "quant_stock.price_daily_r" + str(suffix)
        if not start_dt:
            start_dt = datetime.strptime('2013-01-01', '%Y-%m-%d').date()
        if not end_dt:
            end_dt = datetime.today().date()
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
                logging.info(index, row)
                continue
            sql = 'replace into ' + table_name + " values(\'" + stock_id + "\',\'" + dt + "\'," + str(
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

    def refresh_trade_days(self, ds):
        # 先清空再插入，只支持全量操作
        self._clear_table("quant_stock.stock_trade_days")
        for day in ds:
            sql = "insert ignore into quant_stock.stock_trade_days values(\'" + str(day) + "\')"
            self._cursor.execute(sql)
        self._conn.commit()

    def get_trade_days(self, start_date=None, end_date=None):
        if not start_date:
            start_date = datetime.strptime('2013-01-01', '%Y-%m-%d').date()
        if not end_date:
            end_date = datetime.today().date()
        sql = "select * from quant_stock.stock_trade_days where trade_date >= \'" + str(
            start_date) + "\' and trade_date <= \'" + str(end_date) + "\' order by trade_date"
        self._cursor.execute(sql)
        res = []
        for (dt,) in self._cursor.fetchall():
            res.append(dt)
        return res

    def refresh_stock_info(self, stock_info):
        # 先清空再插入，只支持全量操作
        self._clear_table('quant_stock.stock_info')
        commit_count = 0
        for stock_id, info in stock_info.items():
            ext_str = json.dumps(info['ext'])
            sql = 'insert into quant_stock.stock_info values(\'' + stock_id + '\',\'' + info['display_name'] + '\',\'' + \
                  info['name'] + '\',\'' + str(info['start_date']) + '\',\'' + str(
                info['end_date']) + '\',\'' + ext_str + '\')'
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

    def clear_tbf(self):
        self._clear_table("quant_stock_status.tbf_daily")

    def insert_tbf(self, stock_id, tbf_list):
        if len(tbf_list) == 0:
            return
        sql = 'insert ignore into quant_stock_status.tbf_daily values(\'' + stock_id + '\',\'' + str(
            tbf_list[0]) + '\',\'' + str(tbf_list[-1]) + '\')'
        self._cursor.execute(sql)
        self._conn.commit()

    def get_tbf(self, count=0):
        sql = "select stock_id,start_date,end_date from quant_stock_status.tbf_daily"
        if count > 0:
            sql += " limit " + str(count)
        self._cursor.execute(sql)
        res = self._cursor.fetchall()
        return res

    def remove_tbf(self, key_tuple):
        sql = "delete from quant_stock_status.tbf_daily where stock_id=\'" + key_tuple[0] + "\'" + " and start_date=\'" + str(
            key_tuple[1]) + "\'and end_date=\'" + str(key_tuple[2]) + "\'"
        self._cursor.execute(sql)
        self._conn.commit()

    def refresh_param_space(self, param_space):
        self._clear_table("quant_stock_status.param_space")
        commit_count = 0
        for ps in param_space:
            param_str_list = []
            for i in ps:
                param_str_list.append(str(i))
            sql = 'insert into quant_stock_status.param_space values(\'' + "_".join(param_str_list) + '\', 0)'
            self._cursor.execute(sql)
            commit_count += 1
            if commit_count == 100:
                self._conn.commit()
                commit_count = 0
        self._conn.commit()

    def get_param(self, status, count):
        sql = "select param_id from quant_stock_status.param_space where status=" + str(
            status) + " order by rand() limit " + str(count)
        self._cursor.execute(sql)
        raw_res = self._cursor.fetchall()
        if len(raw_res) == 0:
            return None
        res_total = []
        for (i,) in raw_res:
            pids = i.split("_")
            res = []
            for pid in pids:
                res.append(int(pid))
            res_total.append((i, res))
        return res_total

    def updata_param_status(self, pid, status):
        sql = "update quant_stock_status.param_space set status=" + str(status) + " where param_id=\'" + pid + "\'"
        self._cursor.execute(sql)
        self._conn.commit()

    def refresh_sw_industry_code(self, sw_codes):
        # 先清空再插入，只支持全量操作
        table_name = "quant_stock.sw_industry_code"
        self._clear_table(table_name)
        commit_count = 0
        for (iid, detail) in sw_codes:
            sql = 'insert into ' + table_name + ' values(' + str(iid) + ', \'' + detail + '\')'
            self._cursor.execute(sql)
            commit_count += 1
            if commit_count == 100:
                self._conn.commit()
                commit_count = 0
        self._conn.commit()

    def __del__(self):
        self._cursor.close()
        self._conn.close()
