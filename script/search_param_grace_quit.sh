#!/bin/bash

pid=`ps afx|grep "python backtest.py --search-param" |grep -v "\_"|awk '{print $1}'`
if [ -z $pid ]; then
	echo "Process NOT exsited."
	exit
fi

kill -s SIGUSR1 $pid

while true
do
	ps afx|grep $pid|grep -v "\_" > /dev/null
	if [ $? -eq 0 ]; then
		echo "Grace Quitting...."
		sleep 5
		continue
	else
		echo "Grace Quit Done."
		break
	fi 
done

