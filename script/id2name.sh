#!/bin/bash

stockname=$1

sql="select * from quant_stock.stock_info where cn_name = '"$stockname"'"
mysql -uroot -e "$sql"
