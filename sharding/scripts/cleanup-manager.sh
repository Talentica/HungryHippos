#!/bin/bash
for node in "104.236.33.13"
do
   echo "Cleaning HungryHippos node $node"
   sshpass -p 'Ganesh11' ssh root@$node "cd hungryhippos/manager;rm nohup*;rm Application.log;rm keyCombinationNodeMap;rm keyValueNodeNumberMap"
done
