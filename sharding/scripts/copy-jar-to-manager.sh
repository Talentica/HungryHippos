#!/bin/bash
for node in "104.236.33.13"
do
   echo "Copying file to $node"
   sshpass -p 'Ganesh11' scp ~/$1 root@$node:hungryhippos/manager
done
