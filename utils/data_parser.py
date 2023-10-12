import xlrd

from utils.db_tool import *


def _load_industry_code_file(db_tool, file):
    f = xlrd.open_workbook(file)
    table = f.sheets()[0]
    res = []
    for row in table:
        if row[0].value == '行业代码':
            continue
        cid = int(row[0].value)
        indus_level_1 = row[1].value
        if indus_level_1 == "":
            indus_level_1 = 'NA'
        indus_level_2 = row[2].value
        if indus_level_2 == "":
            indus_level_2 = 'NA'
        indus_level_3 = row[3].value
        if indus_level_3 == "":
            indus_level_3 = 'NA'
        detail = indus_level_1 + "|" + indus_level_2 + "|" + indus_level_3
        res.append((cid, detail))
    db_tool.refresh_sw_industry_code(res)


def _attach_sw_code(db_tool, file):
    f = xlrd.open_workbook(file)
    table = f.sheets()[0]
    stock_industry_map = {}
    for row in table:
        stock_id = row[0].value
        industry_id = row[2].value
        if stock_id not in stock_industry_map:
            stock_industry_map[stock_id] = [industry_id]
        else:
            stock_industry_map[stock_id].append(industry_id)
    stocks = db_tool.get_stock_info(['stock_id', 'ext'])
    update_dict = {}
    for (stock, ext) in stocks:
        pure_id = stock.split('.')[0]
        if pure_id in stock_industry_map:
            ext_dict = json.loads(ext)
            ext_dict["SW_Code"] = stock_industry_map[pure_id]
            update_dict[stock] = json.dumps(ext_dict)
        else:
            print(pure_id + " Missed")
    db_tool.update_stock_info_ext(update_dict)


if __name__ == '__main__':
    dbtool = DBTool(conf_dict['Mysql']['Host'], conf_dict['Mysql']['Port'], conf_dict['Mysql']['User'],
                    conf_dict['Mysql']['Passwd'])
    _load_industry_code_file(dbtool, conf_dict["DataSource"]["SW_Industry_Code_File"])
    _attach_sw_code(dbtool, conf_dict["DataSource"]["SW_Stock_Code_File"])
