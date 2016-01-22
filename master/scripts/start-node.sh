#!/bin/bash
cat ./../../utility/src/main/resources/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

echo "Starting HungryHippos node $1"
sshpass -p $node_pwd ssh root@$1 "sh -c 'cd hungryhippos;nohup java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./ -cp node.jar:test-jobs.jar com.talentica.hungryHippos.manager.node.NodeStarter ./config.properties > /dev/null 2>&1 &'"
