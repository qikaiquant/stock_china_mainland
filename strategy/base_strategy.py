class BaseStrategy:
    """
    prepare：在其中可以做一些策略前置工作，如load模型/规则文件等
    """

    def prepare(self):
        pass

    """
    traversal：遍历股票库，选出备选池，供pick函数使用
    参数：
    stock_list：Stock4Pick组成的列表
    """

    def traversal(self, stock_list):
        pass

    """
    pick：从traversal选出的备选股票池中，进一步筛选，并最终算出需要建仓的股票
    参数：
    stock_list：Stock4Pick组成的列表
    ps_list：Position组成的列表
    """

    def pick(self, stock_list, ps_list):
        print("I am father pick")
        pass

    def update(self):
        pass

    def backtest(self, btc):
        pass

    def visualize(self, res):
        pass
