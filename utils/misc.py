from urllib import request
from urllib.parse import urlencode
import configparser as cp
import utils.db_tool
from hashlib import md5
import os
import time

Msg_Base_Url = 'http://www.pushplus.plus/send?token=dbe8cc80aa704ae88e48e8769b786cc2&'
Stock_DB_Tool = None


def send_wechat_message(title, content):
    http_params = {'title': title, 'content': content}
    url = Msg_Base_Url + urlencode(http_params)
    res = request.urlopen(url)
    print(res.read().decode())


def stockid2table(stockid, base):
    obj = md5()
    obj.update(stockid.encode("UTF-8"))
    hc = obj.hexdigest()
    return int(hc[-4:], 16) % base


'''
一次性使用，将抓取到的文件内数据灌库。弃用。
'''


def file2db():
    cf = cp.ConfigParser()
    cf.read("../config/config.ini")
    # 初始化数据库
    host = cf.get("Mysql", 'Host')
    port = int(cf.get("Mysql", 'Port'))
    user = cf.get("Mysql", 'User')
    passwd = cf.get("Mysql", 'Passwd')
    Stock_DB_Tool = utils.db_tool.DBTool(host, port, user, passwd)
    files = os.listdir("D:\\BaiduNetdiskDownload\\price")
    i = 0
    for file in files:
        sid = file[:11].replace('_', '.')
        tid = stockid2table(sid, 10)
        table_name = 'quant_stock.price_daily_r' + str(tid)

        fp = open("D:\\BaiduNetdiskDownload\\price\\" + file, 'r')
        print("File " + file + " Started@" + table_name)
        line = fp.readline()
        T1 = time.time()
        count = 0
        sqls = []
        while line:
            price = line.strip().split('\t')
            if price[0] == 'code':
                line = fp.readline()
                continue
            dt = price[0].split()[0]
            paused = int(float(price[12]))
            sql = 'insert into ' + table_name + " values(\'" + sid + "\',\'" + dt + "\'," + price[1] + "," + price[
                2] + "," + price[3] + "," + price[4] + "," + price[5] + "," + price[6] + "," + price[7] + "," + price[
                      8] + "," + price[9] + "," + price[10] + "," + price[11] + "," + str(paused) + ")"
            count += 1

            if count == 60:
                Stock_DB_Tool.execute_raw_sql(sqls, True)
                count = 0
                sqls = []
            sqls.append(sql)

            line = fp.readline()
        Stock_DB_Tool.execute_raw_sql(sqls, True)
        T2 = time.time()
        print("File " + file + " Finished:" + str((T2 - T1)))
        fp.close()


if __name__ == '__main__':
    file2db()
