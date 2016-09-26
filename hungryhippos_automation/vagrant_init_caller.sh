#!/bin/bash

source vagrant_init_functions.sh

no_of_nodes=$1
no_of_zookeeper=$2
zookeeperip_string=""

#add ssh key to local machine to access nodes
eval `ssh-agent -s`
ssh-add hhuser_id_rsa

start_vagrantfile $no_of_nodes $no_of_zookeeper

#get all IP
file_processing_to_getIP

#get all zookeeper ip
File_processing_to_get_zookeeper_ip

#retrieve all Ips 
ips=($(awk -F ':' '{print $1}' ip_file_tmp.txt))

#retrieve all zookeeper Ips
zookeeper_ips=($(awk -F ':' '{print $1}' ip_file_zookeeper.txt))

mkdir -p distr

#Copy original distr/* to distr/
cp -r distr_original/* distr/

#create a string in format of zookeperIP:2181,zookeperIP1:2181
create_zookeeperip_string zookeeper_ips[@]

#update distr/client-config.xml with zookeeperip_string
update_client-config

#update distr/cluster-config.xml
update_cluster-config

for ip in "${ips[@]}"
do
#Add all the nodes to known host of this machine
ssh -o StrictHostKeyChecking=no hhuser@$ip "exit"
done

#Creating file to append it to zoo.conf
zookeeper_flag=1

#get total count of zookeeper
total_zookeepers=$(wc -l ip_file_zookeeper.txt | tr -d 'a-z/_/.')

for zookeeper_ip in "${zookeeper_ips[@]}"
do

        echo "server.$zookeeper_flag=$zookeeper_ip:2888:3888" >> append_to_zoo_cfg
        zookeeper_flag=`expr $zookeeper_flag + 1`
        
done

echo "${zookeeper_ips[@]}"

echo "${ips[@]}"

zookeeper_flag=1
for ip in "${ips[@]}"
do
	scp -r distr root@$ip:/home/hhuser/
	ssh root@$ip 'chown hhuser:hungryhippos -R /home/hhuser/distr'
        ssh root@$ip 'echo CLASSPATH=/home/hhuser/distr/lib >> /home/hhuser/.bashrc '
		
	if [ $zookeeper_flag -le $total_zookeepers ] 
	then
	cat append_to_zoo_cfg| ssh hhuser@$ip 'cat  >> /home/hhuser/zookeeper-3.5.1-alpha/conf/zoo.cfg '
	#Start zookeeper
	ssh hhuser@$ip 'cd /home/hhuser/zookeeper-3.5.1-alpha/bin && ./zkServer.sh start '
	fi

	
        zookeeper_flag=`expr $zookeeper_flag + 1`

done

tmp=0
#run scripts on all nodes
for ip in "${ips[@]}"
do

	#Run coordination and torrent tracker shell scripts on random node (here its first node)
	if [ $tmp -eq 0 ] 
	then
		run_script_on_random_node $zookeeperip_string $ip
	fi

	#Run scripts on all node
	run_all_scripts $zookeeperip_string $ip

	tmp=`expr $tmp + 1`

done

rm append_to_zoo_cfg

