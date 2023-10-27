import json
import logging
import platform
import sys
from datetime import datetime, timedelta
from hashlib import md5
from urllib import request
from urllib.parse import urlencode

_Msg_Base_Url = 'http://www.pushplus.plus/send?token=dbe8cc80aa704ae88e48e8769b786cc2&'
OS_TYPE = platform.system()


def _init_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    log_formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)d]%(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')
    # 生产环境，输出到log文件
    if OS_TYPE == 'Linux':
        handler = logging.FileHandler("../log/quant_stock.log")
    # 测试环境，输出到标准输出
    else:
        handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(log_formatter)
    logger.addHandler(handler)


def send_wechat_message(title, content):
    http_params = {'title': title, 'content': content}
    url = _Msg_Base_Url + urlencode(http_params)
    res = request.urlopen(url)
    print(res.read().decode())


def stockid2table(stockid, base=10):
    obj = md5()
    obj.update(stockid.encode("UTF-8"))
    hc = obj.hexdigest()
    return int(hc[-4:], 16) % base


def pid2param(pid):
    str_param = pid.split("_")
    param = []
    for p in str_param:
        param.append(int(p))
    return param


def get_preN_tds(all_trade_days, cur_day, days):
    res = []
    for i in range(1, len(all_trade_days)):
        t = cur_day - timedelta(days=i)
        if t in all_trade_days:
            res.append(t)
        if len(res) == days:
            break
    return res


def _load_config(file):
    # 载入原始配置
    with open(file) as config_file:
        cd = json.load(config_file)
    # 整理操作系统
    if OS_TYPE == 'Linux':
        cd['Mysql']['Host'] = 'localhost'
        cd['Redis']['Host'] = 'localhost'
    # 整理日期
    dt = cd['Backtest']['Start_Date']
    cd['Backtest']['Start_Date'] = datetime.strptime(dt, '%Y-%m-%d').date()
    dt = cd['Backtest']['End_Date']
    cd['Backtest']['End_Date'] = datetime.strptime(dt, '%Y-%m-%d').date()
    return cd


conf_dict = _load_config("../config/config.json")
_init_logger()
