#!/bin/bash

cat ./../../utility/src/main/resources/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt

sh start-zk-server.sh
for node in `cat node_ips_list.txt`
do
echo "Starting HungryHippos node $node"
ssh -o StrictHostKeyChecking=no root@$node "sh -c 'cd hungryhippos;java -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./ -cp node.jar:test-jobs.jar com.talentica.hungryHippos.node.DataReceiver ./config.properties > ./system.out 2>./system.err &'"
done