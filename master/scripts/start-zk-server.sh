#!/bin/bash

zk_node_ip=`cat ./../../utility/src/main/resources/config.properties|grep "zookeeper.server.ips"| awk -F"=" '{print $2}'| awk -F":" '{print $1}'`

node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

for node in `echo $zk_node_ip`
do
   echo "Starting zookeeper server on $node"
   sshpass -p $node_pwd ssh root@$node "zkServer.sh start"
done
