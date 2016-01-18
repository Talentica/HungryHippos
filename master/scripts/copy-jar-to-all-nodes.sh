#!/bin/bash

cat $HOME/git-1.8.1.2/HungryHippos/manager/bin/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt
node_pwd=`cat $HOME/git-1.8.1.2/HungryHippos/manager/scripts/node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

for node in `cat node_ips_list.txt`
do
   echo "Copying file to $node"
   sshpass -p $node_pwd scp ~/$1 root@$node:hungryhippos
done
