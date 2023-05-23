from urllib import request
from urllib.parse import urlencode

Msg_Base_Url = 'http://www.pushplus.plus/send?token=dbe8cc80aa704ae88e48e8769b786cc2&'


def send_wechat_message(title, content):
    http_params = {'title': title, 'content': content}
    url = Msg_Base_Url + urlencode(http_params)
    res = request.urlopen(url)
    print(res.read().decode())
