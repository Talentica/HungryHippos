#!/bin/bash

cat ./../../utility/src/main/resources/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

for node in `cat node_ips_list.txt`
do
	echo "Starting HungryHippos node $1"
	sshpass -p $node_pwd ssh root@$1 "cd hungryhippos;nohup java -cp ./*.jar -jar node.jar ./config.properties &"
done
