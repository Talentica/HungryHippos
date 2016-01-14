#!/bin/bash
for node in "104.131.123.102" "159.203.78.25" "159.203.118.58" "159.203.101.202" "159.203.106.141" "159.203.109.187" "159.203.121.177" "45.55.55.223" "159.203.121.88" "159.203.103.130" "159.203.122.220" "159.203.120.222"
do
   echo "Creating project directory on $node"
   sshpass -p $1 ssh root@$node "mkdir hungryhippos"
done
