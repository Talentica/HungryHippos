#!/bin/bash
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`
manager_node_ip=`cat ./node_pwd_file.txt|grep "manager_node_ip"|awk -F":" '{print $2}'`
echo "Creating required project directories on manager $manager_node_ip"
sshpass -p $node_pwd ssh root@$manager_node_ip "mkdir hungryhippos"
sshpass -p $node_pwd ssh root@$manager_node_ip "mkdir hungryhippos/manager"
