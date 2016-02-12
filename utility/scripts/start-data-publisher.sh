#!/bin/bash

data_publisher_node_ip=`cat ./node_pwd_file.txt|grep "data_publisher_node_ip"|awk -F":" '{print $2}'`
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

for node in `echo $data_publisher_node_ip`
do
   echo "Starting data publisher $node"
   sshpass -p $node_pwd ssh root@$node "cd hungryhippos/data-publisher;java -cp data-publisher.jar:test-jobs.jar com.talentica.hungryHippos.master.DataPublisherStarter $1 &"
done
