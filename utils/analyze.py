from common import *
from redis_tool import *


def parse_stg_detail():
    cachetool = RedisTool(conf_dict['Redis']['Host'], conf_dict['Redis']['Port'], conf_dict['Redis']['Passwd'])
    res = cachetool.get("NW_KEY", 0, serialize=True)
    for _, row in res.iterrows():
        # print(row['dt'])
        # print(row['stg_networth'])
        print(row['details'])


if __name__ == '__main__':
    parse_stg_detail()
