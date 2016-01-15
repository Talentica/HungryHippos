#!/bin/bash
for node in "104.236.33.13"
do
   echo "Starting HungryHippos node $node"
   sshpass -p 'Ganesh11' ssh root@$node "cd hungryhippos/manager;nohup java -jar job-manager.jar &"
done
