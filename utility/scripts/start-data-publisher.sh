#!/bin/bash

manager_node_ip=`cat ./node_pwd_file.txt|grep "manager_node_ip"|awk -F":" '{print $2}'`
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

for node in `echo $manager_node_ip`
do
   echo "Start	ing HungryHippos node $node"
   sshpass -p $node_pwd ssh root@$node "cd hungryhippos/manager;java -cp master.jar:test-jobs.jar com.talentica.hungryHippos.master.DataPublisherStarter $1 &"
done
