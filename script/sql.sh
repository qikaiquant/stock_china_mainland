#!/bin/bash

stockid=$1
limit=$2
if [ -z $limit ]
then
	limit=10
fi

for ((i=0;i<10;i++))
do
	tablename=price_daily_r$i
	echo "Search in Table:"$tablename
	sql="select * from quant_stock."$tablename" where sid = '"$stockid"' order by dt desc limit "$limit
	mysql -uroot -e "$sql"
done
