#!/bin/bash

cat $HOME/git-1.8.1.2/HungryHippos/manager/bin/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt
node_pwd=`cat $HOME/git-1.8.1.2/HungryHippos/manager/scripts/node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

i=0
one=1
for node in `cat node_ips_list.txt`
do
   echo "Creating HungryHippos nodeId for $node"
   sshpass -p $node_pwd ssh root@$node "cd hungryhippos;rm nodeId;echo $i >> nodeId"
   i=$(($i+$one))
   echo $i
done
