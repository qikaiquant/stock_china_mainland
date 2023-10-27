import unittest
import sys
import os
from datetime import datetime

import pandas

sys.path.append(os.path.dirname(sys.path[0]))
from strategy.base_strategy import RES_KEY, COMMON_CACHE_ID
from strategy.macd_strategy import MacdStrategy
from utils.common import conf_dict, pid2param
from utils.db_tool import DBTool
from utils.redis_tool import RedisTool


class STGTest(unittest.TestCase):
    def _stg_flow(self, stg_id, pid, case_file):
        key = RES_KEY + stg_id + ":" + pid
        sdt = datetime.strptime("2022-07-15", '%Y-%m-%d').date()
        edt = datetime.strptime("2023-07-14", '%Y-%m-%d').date()

        dbtool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                        conf_dict['Mysql']['Passwd'])
        redistool = RedisTool(conf_dict['Redis']['Host'], conf_dict['Redis']['Port'], conf_dict['Redis']['Passwd'])

        macd_stg = MacdStrategy(sdt, edt, dbtool, redistool, stg_id)
        macd_stg.reset_param(pid2param(pid))
        macd_stg.backtest(pid)

        test_df = redistool.get(key, COMMON_CACHE_ID, serialize=True)
        # 载入case数据
        case_df = pandas.DataFrame(pandas.read_csv(case_file))
        # 单测过程
        test_res = test_df['stg_nw'].to_list()
        case_res = case_df['stg_nw'].to_list()

        self.assertEqual(len(test_res), len(case_res))
        for i in range(0, len(test_res)):
            self.assertEqual(int(test_res[i]), int(case_res[i]))

    def test_macd(self):
        self._stg_flow("MACD", "1_16_19_15_5", "test_strategy_data_macd.csv")


if __name__ == '__main__':
    unittest.main()
