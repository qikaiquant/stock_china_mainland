import getopt
import importlib
import os
import sys

sys.path.append(os.path.dirname(sys.path[0]))
from utils.common import *


def warm_cache(stg_map, s_list):
    for s in s_list:
        if s not in stg_map:
            logging.warning("STG " + s + " NOT in STG Config.Please CHECK config.json")
            continue
        logging.info("To Warm Cache for STG " + str(s))
        file = importlib.import_module(stg_map[s]['Module_Path'])
        cls = getattr(file, stg_map[s]['Warm_Class_Name'])
        # 初始化实例并执行函数
        clazz = cls(s)
        clazz.warm()


if __name__ == '__main__':
    logging.info("Start Warmer")
    # 预热缓存
    cache_map = conf_dict['STG']
    del cache_map['Base']
    opts, args = getopt.getopt(sys.argv[1:], "", longopts=["all", "stg="])
    for opt, arg in opts:
        # 预热所有db
        if opt == '--all':
            warm_cache(cache_map, cache_map.keys())
        # 预热指定db
        elif opt == '--stg':
            stgs = arg.split(',')
            stg_list = []
            for stg in stgs:
                stg_list.append(stg.strip())
            warm_cache(cache_map, stg_list)
        else:
            logging.error("Usage Error")
            sys.exit(1)
    logging.info("Warmer End")
