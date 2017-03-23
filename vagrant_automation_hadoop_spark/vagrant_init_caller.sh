#!/bin/bash
#*******************************************************************************
# Copyright [2017] [Talentica Software Pvt. Ltd.]
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

source vagrant_init_functions.sh

no_of_nodes=$1
provider=$2

#add ssh key to local machine to access nodes
eval `ssh-agent -s`
ssh-add hduser_id_rsa

#flag
j=0
export MASTER_IP

#updating slave file in chef/ templete as per no of nodes
for (( node_tmp=1; node_tmp <$no_of_nodes; ++node_tmp ))
do

if [ "$node_tmp" -eq 1 ]
then
echo "HadoopSlave1" > chef/src/cookbooks/hadoop_conf_files_setup/templates/default/slaves-template.erb
else
echo "HadoopSlave$node_tmp" >> chef/src/cookbooks/hadoop_conf_files_setup/templates/default/slaves-template.erb
fi

done

# download spark.
download_spark

#download hadoop-2.7.2
download_hadoop

start_vagrantfile $no_of_nodes $provider

file_processing_to_getIP

#retrieve all Ips 
ips=($(awk -F ':' '{print $1}' ip_file_tmp.txt))

echo "${ips[@]}"


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

adding_slave_nodes_to_knownhost_master ips[@]

start_dfs

start_yarn

#upload_to_hdfs /usr/local/hadoop/test.txt /test

#view_data_of_hdfs /

#run_job /usr/local/hadoop/Hadoop-WordCount/wordcount.jar WordCount /test /test_result

#cat_hadoop_result                                                                                                             

#download_from_hdfs /test_result /usr/local/hadoop/test_result.txt

#compare_result

copy_ips_to_slaves ips[@]

start_spark_all ips[@]
