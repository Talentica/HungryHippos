#!/bin/bash
sharding_node_ip=`cat ./node_pwd_file.txt|grep "sharding_node_ip"|awk -F":" '{print $2}'`
sh shut-down-sharding.sh
sh cleanup-sharding.sh
echo 'Copying new build'
sh copy-file-to-sharding.sh ../../sharding/build/libs/sharding*.jar
echo 'Copying common configuration file'
sh copy-file-to-sharding.sh ../../utility/src/main/resources/config.properties
echo 'Copying nodes configuration file'
sh copy-file-to-sharding.sh ../../utility/src/main/resources/serverConfigFile.properties
echo 'Copying data publishers node configuration file'
sh copy-file-to-sharding.sh ./data_publisher_nodes_config.txt
echo 'Copying nodes'' password file'
sh copy-file-to-sharding.sh ./node_pwd_file.txt
echo 'Copying shard file copy utility'
sh copy-file-to-sharding.sh ./copy-shard-files-to-all-nodes.sh
scp ~/.ssh/id_rsa.pub root@$sharding_node_ip:~/.ssh
scp ~/.ssh/id_rsa root@$sharding_node_ip:~/.ssh
