#!/bin/bash

cat ./../../utility/src/main/resources/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

i=0
one=1
for node in `cat node_ips_list.txt`
do
   echo "Creating HungryHippos nodeId for $node"
   sshpass -p $node_pwd ssh root@$node "cd hungryhippos;rm nodeId;echo $i >> nodeId"
   i=$(($i+$one))
done
