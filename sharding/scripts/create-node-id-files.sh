#!/bin/bash
i=0
one=1
for node in "104.236.33.13" "104.236.230.234" "104.236.253.206"
do
   echo "Creating HungryHippos nodeId for $node"
   sshpass -p 'Ganesh11' ssh root@$node "cd hungryhippos;rm nodeId;echo $i >> nodeId"
   i=$(($i+$one))
   echo $i
done
