#!/bin/bash

source vagrant_init_functions.sh

#add ssh key to local machine to access nodes
eval `ssh-agent -s`
ssh-add hhuser_id_rsa

#retrieve all Ips 
ips=($(awk -F ':' '{print $1}' ip_file_tmp.txt))

#retrieve all zookeeper Ips
zookeeper_ips=($(awk -F ':' '{print $1}' ip_file_zookeeper.txt))

create_zookeeperip_string zookeeper_ips[@]

tmp=0


#kill HH related process on all nodes
#for ip in "${ips[@]}"
#do
#ssh hhuser@$ip pkill -f "talentica"
#done


for ip in "${ips[@]}"
do
        scp -r distr hhuser@$ip:/home/hhuser/
        #Run coordination and torrent tracker shell scripts on random node (here its first node)
        if [ $tmp -eq 0 ]
        then
                run_script_on_random_node $zookeeperip_string $ip
        fi


        #Run scripts on all node
        run_all_scripts $zookeeperip_string $ip

        tmp=`expr $tmp + 1`

done

