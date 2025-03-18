import getopt

from utils.common import *

if __name__ == "__main__":
    logging.info("Start Hunter")
    opts, args = getopt.getopt(sys.argv[1:], "", longopts=["prepare", "trade", "test="])
    # 提取test标志,是否上模拟盘：1=模拟盘，0=实盘
    test_flag = 1
    for opt, arg in opts:
        if opt == '--test':
            test_flag = int(arg)
            if test_flag not in [0, 1]:
                logging.error("Test Flag Unvalid")
                sys.exit(1)
        else:
            continue
    # 提取其他
    for opt, arg in opts:
        # 开市前准备
        if opt == '--prepare':
            logging.info("Prepare")
        # 交易
        elif opt == '--trade':
            logging.info("Trade")
    logging.info("End Hunter")
