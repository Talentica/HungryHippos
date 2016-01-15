#!/bin/bash
for node in "104.236.253.206" "104.236.230.234" "104.236.33.13"
do
   echo "Stopping HungryHippos node $node"
   sshpass -p 'Ganesh11' ssh root@$node "killall java"
done
