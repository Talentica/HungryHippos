#!/bin/bash

data_publisher_node_ip=`cat ./node_pwd_file.txt|grep "data_publisher_node_ip"|awk -F":" '{print $2}'`
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`
sh shut-down-all-nodes.sh 
sh start-zk-server.sh
for node in `echo $data_publisher_node_ip`
do
   echo "Starting data publisher $node"
   sshpass -p $node_pwd ssh root@$node "cd hungryhippos/data-publisher;java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./ -cp data-publisher.jar:test-jobs.jar com.talentica.hungryHippos.master.DataPublisherStarter $1 > ./system.out 2>./system.err &"
done
sh start-all-nodes.sh 
