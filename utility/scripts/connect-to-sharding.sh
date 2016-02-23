#!/bin/bash
sharding_node_ip=`cat ./node_pwd_file.txt|grep "sharding_node_ip"|awk -F":" '{print $2}'`

ssh -o StrictHostKeyChecking=no root@$sharding_node_ip
