#!/bin/bash
echo 'Shutting down all java processes on data publisher node'
data_publisher_node_ip=`cat ./node_pwd_file.txt|grep "data_publisher_node_ip"|awk -F":" '{print $2}'`
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

for node in `echo $data_publisher_node_ip`
do
   echo "Stopping sharding on $node"
   sshpass -p $node_pwd ssh root@$node "killall java"
done
