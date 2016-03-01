#!/bin/bash
cat ./../../utility/src/main/resources/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt
for node in `cat node_ips_list.txt`
do
   echo "Clearing buffers on node $node"
   ssh -o StrictHostKeyChecking=no root@$node "echo 1 > /proc/sys/vm/drop_caches"
done
