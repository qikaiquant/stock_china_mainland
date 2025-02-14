import os
import sys
import unittest

import pandas

sys.path.append(os.path.dirname(sys.path[0]))
from strategy.base_strategy import RES_KEY_PREFIX, COMMON_CACHE_ID
from discard.macd_strategy import MacdStrategy
from utils.common import pid2param


class STGTest(unittest.TestCase):
    def _stg_flow(self, stg_id, pid, case_file):
        key = RES_KEY_PREFIX + stg_id + ":" + pid

        macd_stg = MacdStrategy(stg_id)
        macd_stg.reset_param(pid2param(pid))
        macd_stg.backtest(pid)

        test_df = macd_stg.cache_tool.get(key, COMMON_CACHE_ID, serialize=True)
        # 载入case数据
        case_df = pandas.DataFrame(pandas.read_csv(case_file))
        # 单测过程
        test_res = test_df['stg_nw'].to_list()
        case_res = case_df['stg_nw'].to_list()

        self.assertEqual(len(test_res), len(case_res))
        for i in range(0, len(test_res)):
            self.assertEqual(int(test_res[i]), int(case_res[i]))

    def test_macd(self):
        # 必须用参数Start_Date : "2022-07-15","End_Date": "2023-07-14"跑
        self._stg_flow("MACD", "1_16_19_15_5", "../data/test_strategy_data_macd.csv")


if __name__ == '__main__':
    unittest.main()
