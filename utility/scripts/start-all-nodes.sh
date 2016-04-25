#!/bin/bash
jobUuid=$1
cat ../$jobUuid/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt

sh start-zk-server.sh $jobUuid
sh remove-ssh-keygen.sh $jobUuid
for node in `cat node_ips_list.txt`
do
echo "Starting HungryHippos node $node"
ssh -o StrictHostKeyChecking=no root@$node "sh -c 'cd hungryhippos/node;java -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.authenticate=false -Djava.rmi.server.hostname=$node -cp node.jar:test-jobs.jar com.talentica.hungryHippos.node.DataReceiver > ./system.out 2>./system.err &'"
done
