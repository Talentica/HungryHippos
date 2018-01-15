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
source vagrant_init_functions.sh

no_of_nodes=$NODENUM
provider=$PROVIDER

#add ssh key to local machine to access nodes
eval `ssh-agent -s`
ssh-add $PRIVATE_KEY_PATH

#flag
j=0
export MASTER_IP

echo "HadoopMaster" > chef/src/cookbooks/hadoop_conf_files_setup/templates/default/slaves-template.erb

#updating slave file in chef/ templete as per no of nodes
for (( node_tmp=1; node_tmp < $no_of_nodes; ++node_tmp ))
do

echo "HadoopSlave$node_tmp" >> chef/src/cookbooks/hadoop_conf_files_setup/templates/default/slaves-template.erb

done

# download spark.
download_spark

#download hadoop-2.7.2
download_hadoop

cat $PRIVATE_KEY_PATH > chef/src/cookbooks/hadoop_ssh_keygen_master/templates/default/id_rsa
cat $PUBLIC_KEY_PATH > chef/src/cookbooks/hadoop_ssh_keygen_master/templates/default/id_rsa.pub
cat $PUBLIC_KEY_PATH > chef/src/cookbooks/hadoop_ssh_keygen_master/templates/default/authorized_keys.txt
cat $PUBLIC_KEY_PATH > chef/src/cookbooks/hadoop_ssh_keycopy_slave/templates/default/authorized_keys.txt


start_vagrantfile $no_of_nodes $provider

fetch_ip_address(){
    k=1
    val=""
    node_name="HadoopMaster"
    ip_address="$(vagrant ssh $node_name -c 'ifconfig eth0 | grep -oP "inet addr:\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}" | grep -oP "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}" | head -n 1'| tr -d '\r')"
    val+="$ip_address\t$node_name"

    val+='\n'
    while [ $k -lt $NODENUM ]
    do
    node_name="HadoopSlave$k"
    ip_address="$(vagrant ssh $node_name -c 'ifconfig eth0 | grep -oP "inet addr:\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}" | grep -oP "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}" | head -n 1'| tr -d '\r')"
    val+="$ip_address\t$node_name"

    val+='\n'

    k=`expr $k + 1`
    done
echo -e $val > ip_file_hadoop_tmp.txt
}

fetch_ip_address
sed '$d' ip_file_hadoop_tmp.txt > ip_file.txt

file_processing_to_getIP

#retrieve all Ips 
ips=($(awk -F ':' '{print $1}' ip_file_tmp.txt))

echo "${ips[@]}"

MASTER_IP=$(awk -F ':' '/HadoopMaster/{print $1}' ip_file_tmp.txt)

ssh -o StrictHostKeyChecking=no root@$MASTER_IP "sed --in-place '/HadoopMaster/d' /etc/hosts"
#Copying pub key of chef-solo/this machine to master nodes hduser's authorised key.
#Here autorised key of master nodes are same as chef-solo server
ssh root@$MASTER_IP "cat /root/.ssh/authorized_keys >> /home/hduser/.ssh/authorized_keys"
sleep 1

#to copy all Ips to /etc/hosts of every node
copy_ips_to_remote_host ips[@]

#rm ip_file_tmp.txt

#get_master_ip

#Copying test file and test job
scp chef/src/cookbooks/hadoop_master_conf_setup/files/default/expected_test_result.txt  root@$MASTER_IP:/usr/local/hadoop/
scp chef/src/cookbooks/hadoop_master_conf_setup/files/default/Hadoop-WordCount.zip  root@$MASTER_IP:/usr/local/hadoop/
scp chef/src/cookbooks/hadoop_master_conf_setup/files/default/pg20417.txt  root@$MASTER_IP:/usr/local/hadoop/test.txt


#install unzip on master machine
#ssh root@$MASTER_IP 'sudo apt-get install unzip'

#unzip test job file
#ssh root@$MASTER_IP 'unzip /usr/local/hadoop/Hadoop-WordCount.zip -d /usr/local/hadoop/'


format_namenode

hostnames=($(awk -F ':' '{print $2}' ip_file_tmp.txt))

adding_slave_nodes_to_knownhost_master hostnames[@]

start_dfs

start_yarn

#upload_to_hdfs /usr/local/hadoop/test.txt /test

#view_data_of_hdfs /

#run_job /usr/local/hadoop/Hadoop-WordCount/wordcount.jar WordCount /test /test_result

#cat_hadoop_result                                                                                                             

#download_from_hdfs /test_result /usr/local/hadoop/test_result.txt

#compare_result

copy_ips_to_slaves ips[@]

ip_entries=($(awk '{print}' ip_file_tmp.txt))

add_hostname_to_spark_conf ip_entries[@]

start_spark_all ips[@]
