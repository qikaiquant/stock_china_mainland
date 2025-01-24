from utils.common import *

from strategy.base_strategy import BaseStrategy


class TDStatus(Enum):
    TJ = 0,
    MG = 1


class TJMGStrategy(BaseStrategy):
    def __init__(self, stg_id, pid):
        super().__init__(stg_id, pid)
        self.td_status = TDStatus.MG
        self.tj_count = 0

    def is_tj_day(self, dt):
        return True

    def adjust_position(self, dt):
        if self.td_status == TDStatus.MG:
            if self.is_tj_day(dt):
                pass

    def survey(self):
        pass

    def build_param_space(self):
        pass
