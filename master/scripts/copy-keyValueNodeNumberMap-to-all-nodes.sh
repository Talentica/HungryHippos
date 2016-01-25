#!/bin/bash

manager_node_ip=`cat ./node_pwd_file.txt|grep "manager_node_ip"|awk -F":" '{print $2}'`
cat ./../../utility/src/main/resources/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

for node in `cat node_ips_list.txt`
do
echo "Copying KeyValueNodeNumberMap file to node $node"
sshpass -p $node_pwd ssh root@$manager_node_ip "sshpass -p $node_pwd scp /root/hungryhippos/manager/keyValueNodeNumberMap root@$node:/root/hungryhippos"
done
