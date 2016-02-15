#!/bin/bash

cat ./../../utility/src/main/resources/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`
echo "Installing Java on $1"
sshpass -p $node_pwd ssh root@$1 "sudo dpkg --configure -a;sudo add-apt-repository ppa:webupd8team/java -y;sudo apt-get update;nohup sudo apt-get install oracle-java8-installer"
