#!/bin/bash
sh start-zk-server.sh
sharding_node_ip=`cat ../tmp/master_ip_file`

for node in `echo $sharding_node_ip`
do
   echo "Starting sharding on $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/sharding;java -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./ -cp sharding.jar com.talentica.hungryHippos.sharding.main.ShardingStarter > ./system.out 2>./system.err &"
done	
