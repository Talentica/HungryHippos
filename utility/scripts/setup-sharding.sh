#!/bin/bash
jobuuid=$1
sharding_node_ip=`cat ../$jobuuid/master_ip_file`
sh shut-down-sharding.sh $jobuuid
sh cleanup-sharding.sh $jobuuid
echo 'Copying new build'
sh copy-file-to-sharding.sh ../lib/sharding*.jar $jobuuid
echo 'Copying common configuration file'
echo 'Copying data publishers node configuration file'
sh copy-file-to-sharding.sh ./data_publisher_nodes_config.txt $jobuuid
echo 'Copying nodes'' password file'
sh copy-file-to-sharding.sh ./node_pwd_file.txt $jobuuid
echo 'Copying shard file copy utility'
sh copy-file-to-sharding.sh ./copy-shard-files-to-all-nodes.sh $jobuuid
sh copy-file-to-sharding.sh start-kazoo-server.sh $jobUuid
scp ~/.ssh/id_rsa.pub root@$sharding_node_ip:~/.ssh
scp ~/.ssh/id_rsa root@$sharding_node_ip:~/.ssh
