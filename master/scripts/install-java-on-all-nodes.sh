#!/bin/bash

cat $HOME/git-1.8.1.2/HungryHippos/manager/bin/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt
node_pwd=`cat $HOME/git-1.8.1.2/HungryHippos/manager/scripts/node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

for node in `cat node_ips_list.txt`
do
   echo "Installing Java on $node"
   sshpass -p $node_pwd ssh root@$node "sudo add-apt-repository ppa:webupd8team/java -y;sudo apt-get update;sudo apt-get install oracle-java8-installer"
done
