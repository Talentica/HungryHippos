#!/bin/bash
data_publisher_node_ip=`cat ./node_pwd_file.txt|grep "data_publisher_node_ip"|awk -F":" '{print $2}'`
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`
sshpass -p $node_pwd ssh root@$data_publisher_node_ip
