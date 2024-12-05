import abc
from abc import abstractmethod

from trade.trader import Position
from utils.common import *
from utils.db_tool import DBTool
from utils.redis_tool import RedisTool


class BaseWarmer(abc.ABC):
    def __init__(self, stg_id):
        self.db_tool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                              conf_dict['Mysql']['Passwd'])
        self.cache_tool = RedisTool(conf_dict['Redis']['Host'], conf_dict['Redis']['Port'],
                                    conf_dict['Redis']['Passwd'])
        self.cache_no = conf_dict['STG'][stg_id]['DB_NO']
        self.warm_start_date = datetime.strptime(conf_dict['STG']["Base"]['Warm_Start_Date'], '%Y-%m-%d').date()
        self.warm_end_date = datetime.strptime(conf_dict['STG']["Base"]['Warm_End_Date'], '%Y-%m-%d').date()

    @abstractmethod
    def warm(self):
        """
        这个函数需要子类重写
        :return:
        """
        pass


class BaseStrategy(abc.ABC):
    """
    当策略由于仿真/实盘时，运行时间必须在交易日的零点之后，因为具体策略里的dt，和当前时间紧密相关
    """

    def __init__(self, stg_id):
        self.stg_id = stg_id
        # 存储定义
        self.db_tool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                              conf_dict['Mysql']['Passwd'])
        self.cache_tool = RedisTool(conf_dict['Redis']['Host'], conf_dict['Redis']['Port'],
                                    conf_dict['Redis']['Passwd'])
        self.cache_no = conf_dict['STG'][stg_id]['DB_NO']
        # 策略基础参数
        self.stop_loss_point = conf_dict['STG']['Base']['StopLossPoint']  # 止损点，-1表示不设置
        self.stop_surplus_point = conf_dict['STG']['Base']['StopSurplusPoint']  # 止盈点，-1表示不设置
        self.total_budget = conf_dict['STG']['Base']['TotalBudget']
        self.max_hold = conf_dict['STG']["Base"]['MaxHold']
        # 持仓状况
        self.position = Position(self.total_budget, self.max_hold)  # 持仓变化
        # 股票/交易日全量信息
        self.all_trade_days = self.db_tool.get_trade_days()
        self.all_stocks = []
        res = self.db_tool.get_stock_info(['stock_id'])
        for (sid,) in res:
            self.all_stocks.append(sid)

    def stop_loss_surplus(self, stock_id, jiage):
        slot = self.position.get_slot(stock_id)
        if slot is None:
            return False
        buy_jiage = slot[1]
        if self.stop_loss_point != -1:
            min_jiage = buy_jiage * (100 - self.stop_loss_point) / 100.0
            if jiage < min_jiage:
                logging.info(
                    "Stop Loss For Stock " + stock_id + " At Price[" + str(jiage) + "](Buy:[" + str(buy_jiage) + "])")
                return True
        if self.stop_surplus_point != -1:
            max_jiage = buy_jiage * (100 + self.stop_surplus_point) / 100.0
            if jiage > max_jiage:
                logging.info(
                    "Stop Surplus For Stock " + stock_id + " At Price[" + str(jiage) + "](Buy:[" + str(
                        buy_jiage) + "])")
                return True
        return False

    @abstractmethod
    def build_param_space(self):
        """
        这个函数需要被子类重写
        :return:
        """
        pass

    @abstractmethod
    def reset_param(self, param):
        """
        这个函数需要被子类重写
        :param param:
        :return:
        """
        pass

    @abstractmethod
    def adjust_position(self, dt):
        """
        这个函数需要被子类重写
        :return:
        """
        pass

    @abstractmethod
    def survey(self):
        """
        这个函数需要被子类重写，不传参，自由度很大
        :return:
        """
        pass
