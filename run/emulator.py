from gmtrade.api import *

if __name__ == '__main__':
    print("Emulator")
    set_token("a0f6f8418b4ceb3b265d82cce35ef7d741efd8c1")
    set_endpoint("api.myquant.cn:9000")
    a1 = account("e9788055-ab26-11ef-9c3d-00163e022aa6")
    login(a1)
    cash = get_cash()
    print(cash)
