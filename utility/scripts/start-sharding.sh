#!/bin/bash
sh start-zk-server.sh
sharding_node_ip=`cat ./node_pwd_file.txt|grep "sharding_node_ip"|awk -F":" '{print $2}'`
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`
for node in `echo $sharding_node_ip`
do
   echo "Starting sharding on $node"
   sshpass -p $node_pwd ssh root@$node "cd hungryhippos/sharding;java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./ -cp sharding.jar com.talentica.hungryHippos.sharding.main.ShardingStarter > ./system.out 2>./system.err &"
done
