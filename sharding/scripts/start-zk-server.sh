#!/bin/bash
for node in "104.236.33.13"
do
   echo "Starting zookeeper server on $node"
   sshpass -p 'Ganesh11' ssh root@$node "zkServer.sh start"
done
