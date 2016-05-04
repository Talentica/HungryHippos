#!/bin/bash
jobUuid=$1
cat ../$jobUuid/serverConfigFile.properties|awk -F":" '{print $2}' > ../$jobUuid/node_ips_list.txt

sh start-zk-server.sh $jobUuid
sh remove-ssh-keygen.sh $jobUuid
for node in `cat ../$jobUuid/node_ips_list.txt`
do
echo "Starting HungryHippos node $node"
ssh -o StrictHostKeyChecking=no root@$node "sh -c 'cd hungryhippos/node;java -cp node.jar:test-jobs.jar com.talentica.hungryHippos.node.DataReceiver $jobUuid > ./system_data_receiver.out 2>./system_data_receiver.err &'"
done
