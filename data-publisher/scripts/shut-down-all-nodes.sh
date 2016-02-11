#!/bin/bash
echo 'Shutting java all java processes on nodes'
cat ./../../utility/src/main/resources/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

for node in `cat node_ips_list.txt`
do
   echo "Stopping HungryHippos node $node"
   sshpass -p $node_pwd ssh root@$node "killall java"
done
