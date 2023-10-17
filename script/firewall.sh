#/bin/bash

firewall-cmd --zone=public --add-port=111/tcp  --permanent
firewall-cmd --zone=public --add-port=2049/tcp --permanent
firewall-cmd --zone=public --add-port=30001/tcp --permanent
firewall-cmd --zone=public --add-port=30002/tcp --permanent
firewall-cmd --zone=public --add-port=30003/tcp --permanent
firewall-cmd --zone=public --add-port=30004/tcp --permanent
firewall-cmd --zone=public --add-port=111/udp --permanent
firewall-cmd --zone=public --add-port=2049/udp --permanent
firewall-cmd --zone=public --add-port=30001/udp --permanent
firewall-cmd --zone=public --add-port=30002/udp --permanent
firewall-cmd --zone=public --add-port=30003/udp --permanent
firewall-cmd --zone=public --add-port=30004/udp --permanent
