#!/bin/bash
jobuuid=$1
echo "uuid:"$jobuuid
sharding_node_ip=`cat ../$1/master_ip_file`
scp ~/.ssh/id_rsa.pub root@$sharding_node_ip:~/.ssh
scp ~/.ssh/id_rsa root@$sharding_node_ip:~/.ssh
sh copy-file-to-sharding.sh start-kazoo-server.sh $jobuuid
sh shut-down-sharding.sh $jobuuid
sh cleanup-sharding.sh $jobuuid
echo 'Copying new build'
sh copy-file-to-sharding.sh ../lib/sharding*.jar $jobuuid
echo 'Copying common configuration file'
echo 'Copying nodes'' password file'
#sh copy-file-to-sharding.sh ../$jobuuid/node_pwd_file.txt $jobuuid
echo 'Copying shard file copy utility'
sh copy-file-to-sharding.sh copy-shard-files-to-all-nodes.sh $jobuuid
