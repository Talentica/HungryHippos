#!/bin/bash

cat ./../../utility/src/main/resources/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt

for node in `cat node_ips_list.txt`
do
	sh start-node.sh $node
done

rm node_ips_list.txt
