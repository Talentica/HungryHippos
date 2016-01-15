#!/bin/bash

for node in "104.236.253.206" "104.236.230.234" "104.236.33.13"
do
   echo "Cleaning up HungryHippos node  $node"
   sshpass -p 'Ganesh11' ssh root@$node "cd hungryhippos;rm data_*;rm Application.log;rm outputFile;rm nohup*"
done
