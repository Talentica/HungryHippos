#!/bin/bash
echo 'Installing java on all nodes'
cat ./../../utility/src/main/resources/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt


for node in `cat node_ips_list.txt`
do
echo "Installing java on node  $node"
sh install-java-on-a-node.sh $node
done