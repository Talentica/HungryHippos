#!/bin/bash
data_publisher_node_ip=`cat ./node_pwd_file.txt|grep "data_publisher_node_ip"|awk -F":" '{print $2}'`

ssh -o StrictHostKeyChecking=no root@$data_publisher_node_ip
