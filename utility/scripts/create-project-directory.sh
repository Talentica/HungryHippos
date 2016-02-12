#!/bin/bash
echo "Creating required project directories"
cat ./../../utility/src/main/resources/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`
manager_node_ip=`cat ./node_pwd_file.txt|grep "manager_node_ip"|awk -F":" '{print $2}'`
for node in `cat node_ips_list.txt`
do
   echo "Creating required project directories on node $node"
   sshpass -p $node_pwd ssh root@$node "mkdir hungryhippos"
   sshpass -p $node_pwd ssh root@$node "mkdir hungryhippos/data"
done
