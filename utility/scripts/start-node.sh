#!/bin/bash
cat ../tmp/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt

sh start-zk-server.sh
echo "Starting HungryHippos node $1"
ssh -o StrictHostKeyChecking=no root@$1 "sh -c 'cd hungryhippos/node;java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./ -cp node.jar:test-jobs.jar com.talentica.hungryHippos.node.DataReceiver > ./system.out 2>./system.err &'"
