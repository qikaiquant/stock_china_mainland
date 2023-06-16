import configparser
import logging
import platform
import sys
from hashlib import md5
from urllib import request
from urllib.parse import urlencode

_Msg_Base_Url = 'http://www.pushplus.plus/send?token=dbe8cc80aa704ae88e48e8769b786cc2&'
_OS_TYPE = platform.system()


def _init_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    prod_formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)d]%(message)s',
                                       datefmt='%Y-%m-%d %H:%M:%S')
    debug_formatter = logging.Formatter('[%(asctime)s][%(levelname)s]%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    # 生产环境，输出到log文件
    if _OS_TYPE == 'Linux':
        fh = logging.FileHandler("../log/quant_stock.log")
        fh.setFormatter(prod_formatter)
        logger.addHandler(fh)
    # 测试环境，输出到标准输出
    else:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(debug_formatter)
        logger.addHandler(sh)


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


def _load_config(file):
    # 载入原始配置
    cd = {}
    cf = configparser.ConfigParser()
    cf.read(file)
    secs = cf.sections()
    for sec in secs:
        cd[sec] = {}
        its = cf.items(sec)
        for k, v in its:
            cd[sec][k] = v
    if _OS_TYPE == 'Linux':
        cd['Mysql']['host'] = 'localhost'
        cd['Redis']['host'] = 'localhost'
    port = int(cd['Mysql']['port'])
    cd['Mysql']['port'] = port
    port = int(cd['Redis']['port'])
    cd['Redis']['port'] = port
    return cd


_init_logger()
conf_dict = _load_config("../config/config.ini")
