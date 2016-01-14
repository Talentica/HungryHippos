#!/bin/bash
for node in "104.236.33.13"
do
   echo "Stopping HungryHippos manager $node"
   sshpass -p 'Ganesh11' ssh root@$node "killall java"
done
