#!/bin/bash

cat ./../../utility/src/main/resources/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

for node in `cat node_ips_list.txt`
do
echo "Starting HungryHippos node $node"
sshpass -p $node_pwd ssh root@$node "sh -c 'cd hungryhippos;nohup java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./ -cp node.jar:test-jobs.jar com.talentica.hungryHippos.manager.node.NodeStarter ./config.properties > /dev/null 2>&1 &'"
done
