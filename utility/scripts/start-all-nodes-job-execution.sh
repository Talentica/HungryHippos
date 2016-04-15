#!/bin/bash

cat ../tmp/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt

sh start-zk-server.sh
for node in `cat node_ips_list.txt`
do
echo "Starting HungryHippos node for job execution $node"
ssh -o StrictHostKeyChecking=no root@$node "sh -c 'cd hungryhippos/node;java -Xmx3800m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./ -cp node.jar:test-jobs.jar com.talentica.hungryHippos.node.JobExecutor > ./system.out 2>./system.err &'"
done
