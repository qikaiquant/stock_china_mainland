import configparser as cp
import importlib as il
import traceback as tb
import data.data_def as df

BTC_Start_Date = None
BTC_End_Date = None


def backtest(strategy):
    stock_arry = []
    position = df.Position()
    print("Start to Backtest For Stragety[" + str(type(strategy)) + "]")
    # 载入规则/模型
    if not strategy.prepare():
        print("Fail to Load Backtest Strategy")
        return
    # 遍历股票池
    if not strategy.traversal(stock_arry):
        print("Fail to Traversel and Score")
        return
    # 模拟建仓
    if not strategy.pick(stock_arry, position):
        print("Fail to Pick")
        return
    # 检查仓位
    if position.isEmpty():
        print("No Position, Exit")
        return
    # 执行回测
    btc = df.BTContext(BTC_Start_Date, BTC_End_Date, position)
    if not strategy.backtest(btc):
        print("BackTest Failed")
        return
    result = btc.get_res()
    if not result:
        print("No BackTest Reulst")
        return
    # 可视化结果
    strategy.visualize(result)


if __name__ == '__main__':
    cf = cp.ConfigParser()
    cf.read("../config/config.ini")

    try:
        file = il.import_module(cf.get("Backtest", "Strategy_Module_Path"))
        cls = getattr(file, cf.get("Backtest", "Strategy_Class_Name"))
        BTC_Start_Date = cf.get("Backtest", "BTC_Start_Date")
        BTC_End_Date = cf.get("Backtest", "BTC_End_Date")
        if (not BTC_Start_Date) or (not BTC_End_Date):
            print("BTC Start or End Date NO Set, Backtest Failed.")
            raise RuntimeError
        backtest(cls())
    except:
        tb.print_exc()
