from urllib import request
from urllib.parse import urlencode
from hashlib import md5

Msg_Base_Url = 'http://www.pushplus.plus/send?token=dbe8cc80aa704ae88e48e8769b786cc2&'
Stock_DB_Tool = None


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
