#!/bin/bash
jobUuid=$1
cat ../$jobUuid/master_ip_file > data_publisher_node_ips.txt
sh shut-down-all-nodes.sh 
sh start-zk-server.sh $jobUuid

for node in `cat data_publisher_node_ips.txt`
do
    ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/sharding;sh copy-shard-files-to-all-nodes.sh"
   echo "Starting data publisher $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/data-publisher;java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./ -cp data-publisher.jar:test-jobs.jar com.talentica.hungryHippos.master.DataPublisherStarter > ./system.out 2>./system.err &"
done

sh start-all-nodes.sh $jobUuid
