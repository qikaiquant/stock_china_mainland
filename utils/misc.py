from urllib import request
from urllib.parse import urlencode
from hashlib import md5

import random
import matplotlib.pyplot as plt

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


def draw():
    x = range(0, 120)
    y1 = [random.randint(25, 30) for i in x]
    y2 = [random.randint(11, 14) for i in x]

    fig = plt.figure(figsize=(10, 6), dpi=100)
    plt.rc('font', family='FangSong', size=14)
    # 左侧折线图
    left, bottom, width, height = 0.03, 0.2, 0.7, 0.6
    ax1 = fig.add_axes([left, bottom, width, height])
    ax1.plot(x, y1, color='darkred', label="Zhang")
    ax1.plot(x, y2, color='slategrey', label="Wang")
    ax1.grid(linestyle='--')
    ax1.set_facecolor('whitesmoke')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    # plt.title("000023.SZ")
    ax1.legend(loc='lower right')
    # 右侧数据指标表格
    left, bottom, width, height = 0.75, 0.2, 0.2, 0.6
    ax2 = fig.add_axes([left, bottom, width, height])
    celltext = [['收益', '120%', '250%'], ['年化收益', '120%', '250%'], ['夏普指数', '0.5', '2'],
                ['最大回撤', '50%', '20%']]
    columns = ['指标', "MA50", "基线"]
    ax2.axis('off')
    tb = ax2.table(cellText=celltext, colLabels=columns, loc='lower left', cellLoc='center', rowLoc='bottom')
    tb.scale(1.1, 1.3)

    # plt.savefig("D:\\test\\aaa.jpg", dpi=600)
    plt.show()
