#!/bin/bash

manager_node_ip=`cat $HOME/git-1.8.1.2/HungryHippos/manager/scripts/node_pwd_file.txt|grep "manager_node_ip"|awk -F":" '{print $2}'`
node_pwd=`cat $HOME/git-1.8.1.2/HungryHippos/manager/scripts/node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

for node in `echo $manager_node_ip`
do
   echo "Starting HungryHippos node $node"
   sshpass -p $node_pwd ssh root@$node "cd hungryhippos/manager;nohup java -jar job-manager.jar &"
done
