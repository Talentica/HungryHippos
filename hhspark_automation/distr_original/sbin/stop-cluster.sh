#!/bin/bash

ips=($(awk '{print $1}' ../config/hhnodes))
for ip in "${ips[@]}"
do
echo 	"stopping Hungry Hippos process in $ip"
	
	ssh hhuser@$ip pkill -f "com.talentica.hungryHippos.node.DataDistributorStarter"
	
done
echo "waiting for stop process to end"
wait
echo "completed stop process"




