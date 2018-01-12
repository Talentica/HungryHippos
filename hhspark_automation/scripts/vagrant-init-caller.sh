#!/bin/bash
#*******************************************************************************
# Copyright 2017 Talentica Software Pvt. Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#*******************************************************************************

source assign-global-variables.sh
source utility.sh
source zookeeper.sh
source spark.sh
source run-hh-scripts-on-cluster.sh
source ../start-vagrant.sh


flag=$(check_file_exists "vagrant.properties")

if [ $flag != "true" ];
then
        echo "not able to find vagrant.properties"
        echo "cp vagrant.properties.template vagrant.properties and override the default values"
        echo "make sure ssh key is added in digital ocean and ~/.ssh/ location"
        exit 
fi


zookeeperip_string=""

#remove known hosts (utility.sh)
remove_known_hosts

#add ssh key to local machine to access nodes
add_ssh_key #(utility.sh)

#copying ssh public key and private key to /chef directory

ssh_key_path=$PRIVATE_KEY_PATH
ssh_key_dir=${ssh_key_path%/*}'/'
cat $PRIVATE_KEY_PATH > ../chef/src/cookbooks/hh_ssh_keygen_master/templates/default/id_rsa
cat $PUBLIC_KEY_PATH > ../chef/src/cookbooks/hh_ssh_keygen_master/templates/default/id_rsa.pub
cat $PUBLIC_KEY_PATH > ../chef/src/cookbooks/hh_ssh_keycopy_slave/templates/default/authorized_keys.txt

#download zookeeper to chef cookbook download_zookeeper (zookeeper.sh)
zookeeper_file_loc="../chef/src/cookbooks/download_zookeeper/files/default/zookeeper-3.5.1-alpha.tar.gz"
flag=$(check_file_exists $zookeeper_file_loc)

if [ $flag != "true" ];
then
	download_zookeeper
fi
spark_file_loc="../chef/src/cookbooks/download_spark/files/default/spark-2.2.0-bin-hadoop2.7.tgz"
# download spark. (spark.sh)

flag=$(check_file_exists $spark_file_loc)

if [ $flag != "true" ];
then
    download_spark
fi

start_vagrantfile #(start-vagrant.sh)

fetch_ip_address(){
    k=1
val=""
    while [ $k -le $NODENUM ]
    do
    node_name="$NAME-$k"
    ip_address="$(vagrant ssh $node_name -c 'ifconfig eth0 | grep -oP "inet addr:\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}" | grep -oP "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}" | head -n 1'| tr -d '\r')"
    val+="$ip_address:$node_name"
    if [ $k -le $ZOOKEEPERNUM ]
    then
       val+=' (Zookeeper)\n'
    else
       val+='\n'
    fi
    k=`expr $k + 1`
    done
echo -e $val >> ip_file_hh_tmp.txt
}

fetch_ip_address
sed '$d' ip_file_hh_tmp.txt > ip_file.txt
rm ip_file_hh_tmp.txt


#get all IP
file_processing_to_get_ip #(utility.sh)

#get all zookeeper ip
file_processing_to_get_zookeeper_ip #(utility.sh)

#retrieve all Ips 
ips=($(awk -F ':' '{print $1}' ip_file.txt))
echo ${ips[@]}
#retrieve all zookeeper Ips
zookeeper_ips=($(awk -F ':' '{print $1}' ip_file_zookeeper.txt))
echo ${zookeeper_ips[@]}

#copy server_scripts to distr_original/bin folder
rm -rf ../distr_original/bin
mkdir ../distr_original/bin
cp  server_scripts/* ../distr_original/bin
mkdir -p ../distr

#Copy original distr/* to distr/
cp -r ../distr_original/* ../distr/
mkdir -p ../distr/logs

#create a string in format of zookeperIP:2181,zookeperIP1:2181
create_zookeeperip_string ${zookeeper_ips[@]}

#update distr/client-config.xml with zookeeperip_string
update_client-config  #(utility.sh)

#update distr/cluster-config.xml
update_cluster-config #(utility.sh)

#add cluster nodes to client machine knownhosts
add_nodes_to_known_hosts # (utility.sh)

append_to_zoo_conf #(utility.sh)

zookeeper_flag=1
for ip in "${ips[@]}"
do

	update_limits_conf	#(utility.sh)
        scp -r ../distr hhuser@$ip:/home/hhuser/
	ssh hhuser@$ip 'chown hhuser:hungryhippos -R /home/hhuser/distr'
        ssh hhuser@$ip 'echo CLASSPATH=/home/hhuser/distr/lib >> /home/hhuser/.bashrc '
		

	if [ $zookeeper_flag -le $total_zookeepers ] 
        then
        	cat append_to_zoo_cfg| ssh hhuser@$ip 'cat  >> /home/hhuser/zookeeper-3.5.1-alpha/conf/zoo.cfg '
 	#Start zookeeper
        	ssh hhuser@$ip 'cd /home/hhuser/zookeeper-3.5.1-alpha/bin && ./zkServer.sh start '
	fi
	
        zookeeper_flag=`expr $zookeeper_flag + 1`

done

tmp=0
#run scripts on all nodes run-hh-scripts-on-cluster.sh

for ip in "${ips[@]}"
do

	#Run coordination shell scripts on random node (here its first node)
	if [ $tmp -eq 0 ] 
	then
		run_script_on_random_node $zookeeperip_string ${ips[0]}
                sleep 5
        fi

	ssh root@$ip "chown root /home/hhuser/distr/bin/clear_unix_cache"
        ssh root@$ip "chmod 7755 /home/hhuser/distr/bin/clear_unix_cache"

        echo zookeeperip_string $zookeeperip_string
	#Run scripts on all node
	run_all_scripts $zookeeperip_string $ip

	tmp=`expr $tmp + 1`

done

#add ip to spark.env files (spark.sh)
add_spark_ip_port_on_spark_env ips[@]

#starts master and slave
start_spark_all ips[@]

rm append_to_zoo_cfg

