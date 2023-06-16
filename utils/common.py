import configparser
import logging
import platform
import sys
from hashlib import md5
from urllib import request
from urllib.parse import urlencode

Msg_Base_Url = 'http://www.pushplus.plus/send?token=dbe8cc80aa704ae88e48e8769b786cc2&'
OS_TYPE = platform.system()


def _init_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    prod_formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)d]%(message)s',
                                       datefmt='%Y-%m-%d %H:%M:%S')
    debug_formatter = logging.Formatter('[%(asctime)s][%(levelname)s]%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    # 生产环境，输出到log文件
    if OS_TYPE == 'Linux':
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
    url = Msg_Base_Url + urlencode(http_params)
    res = request.urlopen(url)
    print(res.read().decode())


def stockid2table(stockid, base=10):
    obj = md5()
    obj.update(stockid.encode("UTF-8"))
    hc = obj.hexdigest()
    return int(hc[-4:], 16) % base


def load_config(file):
    # 载入原始配置
    conf_dict = {}
    cf = configparser.ConfigParser()
    cf.read(file)
    secs = cf.sections()
    for sec in secs:
        conf_dict[sec] = {}
        its = cf.items(sec)
        for k, v in its:
            conf_dict[sec][k] = v
    if OS_TYPE == 'Linux':
        conf_dict['Mysql']['host'] = 'localhost'
        conf_dict['Redis']['host'] = 'localhost'
    port = int(conf_dict['Mysql']['port'])
    conf_dict['Mysql']['port'] = port
    port = int(conf_dict['Redis']['port'])
    conf_dict['Redis']['port'] = port
    return conf_dict


_init_logger()
conf_dict = load_config("../config/config.ini")
