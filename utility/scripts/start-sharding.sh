#!/bin/bash
jobUuid=$1
sh start-zk-server.sh $jobUuid
sharding_node_ip=`cat ../$jobUuid/master_ip_file`

for node in `echo $sharding_node_ip`
do
   echo "Starting process db entry script"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/sharding;java -cp sharding.jar com.talentica.hungryHippos.sharding.main.StartProcessDBEntryMain $jobUuid > ./system_db_process_entry.out 2>./system_db_process_entry.err &"
   echo "Started process db entry"

   echo "Starting kazoo server"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/sharding;java -cp sharding.jar com.talentica.hungryHippos.sharding.main.StartKazooScriptMain $jobUuid > ./system_kazoo_server.out 2>./system_kazoo_server.err &"
   echo "Kazoo Server started"

   echo "Starting sharding on $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/sharding;java -cp sharding.jar com.talentica.hungryHippos.sharding.main.ShardingStarter $jobUuid > ./sharding_system.out 2>./sharding_system.err &"
done
