#!/bin/bash
sh shut-down-data-publisher.sh
cat ./data_publisher_nodes_config.txt|awk -F":" '{print $2}' > data_publisher_node_ips.txt
for node in `cat data_publisher_node_ips.txt`
do
   echo "Generating data file on node $node"
   ssh -o StrictHostKeyChecking=no root@$node "echo 1 > /proc/sys/vm/drop_caches;cd hungryhippos/data-publisher;rm ../input/sampledata_new.txt;java -XX:+HeapDumpOnOutOfMemoryError -Xmx1500m -XX:HeapDumpPath=./ -cp data-publisher.jar com.talentica.hungryHippos.utility.ConfigurableDataGenerator $1 ../input/sampledata_new.txt C:1 C:1 C:1 C:3 C:3 C:3 N:3 N:2 C:5> ./system.out 2>./system.err &"
done
