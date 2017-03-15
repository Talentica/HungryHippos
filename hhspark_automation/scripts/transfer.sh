#!/bin/bash

source assign-global-variables.sh
source utility.sh
source run-hh-scripts-on-cluster.sh

#add ssh key to local machine to access nodes
add_ssh_key
#retrieve all Ips 
ips=($(awk -F ':' '{print $1}' ip_file.txt))

#retrieve all zookeeper Ips
zookeeper_ips=($(awk -F ':' '{print $1}' ip_file_zookeeper.txt))

create_zookeeperip_string zookeeper_ips[@]


start_process(){
tmp=0
for ip in "${ips[@]}"
do

        if [ $tmp -eq 0 ]
        then
                run_script_on_random_node $zookeeperip_string $ip
        fi
	#Run scripts on all node
        run_all_scripts $zookeeperip_string $ip

	tmp=`expr $tmp + 1`

done

}

copy_folder(){

for ip in "${ips[@]}"
do
        scp -r ../distr hhuser@$ip:/home/hhuser/
        ssh root@$ip "chown root /home/hhuser/distr/bin/clear_unix_cache"
        ssh root@$ip "chmod 7755 /home/hhuser/distr/bin/clear_unix_cache"

done

}

