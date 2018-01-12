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

start_vagrantfile()
{
    echo "Calling start_vagrantfile"
	no_of_nodes=$1
	provider=$2

	#Start vagrant file

	if [ "$provider" == "digital_ocean"  ]
        then	
		NODENUM=$no_of_nodes PROVIDER=$provider  vagrant up --provider=digital_ocean
	elif [ "$provider" == "virtual_box"  ]
	then
		NODENUM=$no_of_nodes  PROVIDER=$provider  vagrant up
	fi

	#Start vagrant file
	#NODENUM=$no_of_nodes vagrant up --provider=digital_ocean
	sleep 10
	echo "End of start_vagrantfile"
}


file_processing_to_getIP()
{
	#Copy original file to tmp file to perform file operatiob
	cp ip_file.txt ip_file_tmp.txt


	#replace tab with colon in tmp file
	sed -i 's/	/:/g' ip_file_tmp.txt

	#Sort file  as we want HadoopMaster to come at first line
	sort  -t':' -k2 ip_file_tmp.txt -o ip_file_tmp.txt

}

copy_ips_to_remote_host()
{
	ips=("${!1}")
	echo "${ips[@]}"

	j=0
	for i in "${ips[@]}"
	do
	        echo $i

        	j=`expr $j + 1`


	        chmod 777 ip_file.txt

        	#for adding new ip addresses to known hosts
	        #ssh -o StrictHostKeyChecking=no root@$i
        	ssh-keyscan $i >> ~/.ssh/known_hosts
	        sleep 1
	        ssh -o StrictHostKeyChecking=no root@$i "sed --in-place '/HadoopSlave/d' /etc/hosts"

	        #copy ip file to every node
	        cat ip_file.txt | ssh root@$i "cat >> /etc/hosts"
	        sleep 1

	done
    

}

add_hostname_to_spark_conf()
{
	ips_entries=("${!1}")
	j=0
	for ip_entry in "${ips_entries[@]}"
	do
	        echo $ip_entry
	        ip=$(echo $ip_entry | cut -d ':' -f1)
	        sleep 1
	        hostname=$(echo $ip_entry | cut -d ':' -f2)
	        sleep 1
	        echo $hostname
	        echo $ip
            ssh root@$ip "echo SPARK_LOCAL_HOSTNAME=${hostname} >> /usr/local/spark-2.2.0-bin-hadoop2.7/conf/spark-env.sh"
	        sleep 1

	done
}

get_master_ip()
{

	#retrieve all Ips 
	ips=($(awk -F ':' '{print $1}' ip_file_tmp.txt))

	j=0
	for i in "${ips[@]}"
	do
		j=`expr $j + 1`

		if [ $j -eq 1  ]
	        then
        	        MASTER_IP=$i
	       
			break
		fi

	done

}


adding_slave_nodes_to_knownhost_master()
{

	hostnames=("${!1}")
	#echo "${hostnames[@]}"


	#adding all slave nodes to known host of master
	for hostname in "${hostnames[@]}"
	do

	        addhostname="ssh-keyscan ${hostname} >> ~/.ssh/known_hosts"
       		ssh hduser@$MASTER_IP $addhostname
	        sleep 1

	#below line is to solve warning - "Hadoop “Unable to load native-hadoop library for your platform” warning" on every node
	#ssh hduser@$ip "export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=/usr/local/hadoop/lib/native""
	done

	#adding secondary name node to known host of master
	ssh hduser@$MASTER_IP "ssh -o StrictHostKeyChecking=no hduser@0.0.0.0 "exit""
	sleep 1


}

format_namenode()
{
	#format namenode
	ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hdfs namenode -format"
	sleep 5
	echo "namenode formatted"
}



start_dfs(){
	#start-dfs
	ssh hduser@$MASTER_IP "/usr/local/hadoop/sbin/start-dfs.sh"
	sleep 1
}


start_yarn(){
	#start-yarn
	ssh hduser@$MASTER_IP "/usr/local/hadoop/sbin/start-yarn.sh"
	sleep 1
}


upload_to_hdfs(){
	#copy test file from local machine to hdfs
	#ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /usr/local/hadoop/test.txt /test"

	#get dir name
	DIR=$(dirname "$2")
	ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hdfs dfs -mkdir -p  $DIR"
	ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hdfs dfs -copyFromLocal  $1 $2"
}


view_data_of_hdfs(){
	#List data uploaded to HDFS
	echo "data uploaded to HDFS is"
	ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hdfs dfs -ls $1"
	#ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hdfs dfs -ls /"
}

run_job(){
	#Run jar file of hadoop job
	echo "Running hadoop wordcount job"
	#ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/Hadoop-WordCount/wordcount.jar WordCount /test /test_result"
	ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hadoop jar $1 $2 $3 $4"

}

delete_hdfs(){
	#Delete file on hdfs
        ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hdfs dfs -rm -r $1"


}



cat_hadoop_result(){
	#Check hadoop result
	echo "Displaying result of hadoop wordcount job"
	sleep 4
	ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hdfs  dfs -cat  /test_result/*"
}



download_from_hdfs(){
	#copy file from hdfs system to local
	ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hdfs  dfs -getmerge $1 $2"
}



compare_result(){
	#Compare expected result file with actual result
	diff_in_result=$(ssh hduser@$MASTER_IP "diff /usr/local/hadoop/expected_test_result.txt /usr/local/hadoop/test_result.txt")

	if [ ! $diff_in_result ]; then
		echo "No difference in actual file and expected file"
		echo "Hadoop configuration with testing sample wordcount example completed successfully !!!"
		sleep 5
	else
		echo "There is difference in actual file and expected file, please check it manually."
	fi

}

get_hdfs_file_size_input(){

	file_size_input=$(ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hdfs dfs -du -s  $1")
	file_size_input=$(echo $file_size_input | cut -d' ' -f1)
	NUM=$file_size_input
	file_size_input=$(echo "scale=2;$NUM/1024" | bc)
}

get_hdfs_file_size_output(){

	file_size_output=$(ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hdfs dfs -du -s  $1")
	file_size_output=$(echo $file_size_output | cut -d' ' -f1)
	NUM=$file_size_output
	file_size_output=$(echo "scale=2;$NUM/1024" | bc)


}


job_input_dbwrite(){

	mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "INSERT INTO job_input (job_id,data_location,data_size_in_kbs) VALUES ('$1', '$2','$3');"
}

job_output_dbwrite(){

	mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "INSERT INTO job_output (job_id,data_location,data_size_in_kbs) VALUES ('$1', '$2','$3');"
}

get_process_id(){

	process_id=$(mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "select process_id from process where name='$1';")
	process_id=$(echo $process_id | cut -d' ' -f2)
	#echo $process_id
}

process_instance_dbwrite(){

	mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "INSERT INTO process_instance (process_id,job_id) VALUES ('$1', '$2');"

	#get process instance id
	process_instance_id=$(mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "select process_instance_id from process_instance where process_id='$process_id' AND job_id='$job_id';")
	process_instance_id=$(echo $process_instance_id | cut -d' ' -f2)

}

process_instance_detail_dbwrite(){

	mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "INSERT INTO process_instance_detail (process_instance_id) VALUES ('$1');"

	#get process instance detail id
	process_instance_detail_id=$(mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "select process_instance_detail_id from process_instance_detail where process_instance_id='$process_instance_id';")
	process_instance_detail_id=$(echo $process_instance_detail_id | cut -d' ' -f2)

}

read_json(){

	i=$1

	 #read values from json file
        jar_file_path=$(cat hadoop_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].jar_file_path')
        input_file_path=$(cat hadoop_conf.json  |jq --arg job_no "$i" '.jobs['$i'].input_file_path')
        class_name=$(cat hadoop_conf.json  |jq '.jobs['$i'].class_name')
        output_file_name=$(cat hadoop_conf.json  |jq '.jobs['$i'].output_file_name')
        desired_input_file_location_master=$(cat hadoop_conf.json  |jq '.jobs['$i'].desired_input_file_location_master')
        desired_output_file_location_master=$(cat hadoop_conf.json  |jq '.jobs['$i'].desired_output_file_location_master')
        desired_job_file_location_master=$(cat hadoop_conf.json  |jq '.jobs['$i'].desired_job_file_location_master')
        desired_input_file_location_hdfs=$(cat hadoop_conf.json  |jq '.jobs['$i'].desired_input_file_location_hdfs')
        desired_output_file_location_hdfs=$(cat hadoop_conf.json  |jq '.jobs['$i'].desired_output_file_location_hdfs')
        expected_result_file_path=$(cat hadoop_conf.json  |jq --arg job_no "$i" '.jobs['$i'].expected_result_file_path')
        desired_expected_result_location_master=$(cat hadoop_conf.json  |jq --arg job_no "$i" '.jobs['$i'].desired_expected_result_location_master')
	delete_hdfs_file_name=$(cat hadoop_conf.json  |jq --arg job_no "$i" '.jobs['$i'].delete_hdfs_file')
	mysql_server=$(cat hungryhippos_operations_conf.json  |jq '.mysql_server')
	mysql_username=$(cat hungryhippos_operations_conf.json  |jq '.mysql_username')
	mysql_password=$(cat hungryhippos_operations_conf.json  |jq '.mysql_password')



        #remove " from variable
        jar_file_path=$(echo "$jar_file_path" | tr -d '"')
        input_file_path=$(echo "$input_file_path" | tr -d '"')
        class_name=$(echo "$class_name" | tr -d '"')
        output_file_name=$(echo "$output_file_name" | tr -d '"')
        desired_input_file_location_master=$(echo "$desired_input_file_location_master" | tr -d '"')
        desired_output_file_location_master=$(echo "$desired_output_file_location_master" | tr -d '"')
        desired_job_file_location_master=$(echo "$desired_job_file_location_master" | tr -d '"')
        desired_input_file_location_hdfs=$(echo "$desired_input_file_location_hdfs" | tr -d '"')
        desired_output_file_location_hdfs=$(echo "$desired_output_file_location_hdfs" | tr -d '"')
        expected_result_file_path=$(echo "$expected_result_file_path" | tr -d '"')
        desired_expected_result_location_master=$(echo "$desired_expected_result_location_master" | tr -d '"')
	delete_hdfs_file_name=$(echo "$delete_hdfs_file_name" | tr -d '"')
	mysql_server=$(echo "$mysql_server" | tr -d '"')
	mysql_username=$(echo "$mysql_username" | tr -d '"')
	mysql_password=$(echo "$mysql_password" | tr -d '"')


}

expected_result_file_path_operations(){


	#get file name from path
        expected_result_file=`basename $expected_result_file_path`

        #create folder on master
        ssh root@$MASTER_IP "mkdir -p $desired_expected_result_location_master"

        ssh root@$MASTER_IP "chown hduser:hadoop $desired_expected_result_location_master"

        #copy expected result file to master machine
        scp $expected_result_file_path root@$MASTER_IP:$desired_expected_result_location_master/$expected_result_file


}

master_setup_for_job_execution(){

	#get jar file name from path
        jar_name=`basename $jar_file_path`

        #get file name from path
        file_name=`basename $input_file_path`


        #create folders on master
        ssh root@$MASTER_IP "mkdir -p $desired_input_file_location_master"
        ssh root@$MASTER_IP "mkdir -p $desired_output_file_location_master"
        ssh root@$MASTER_IP "mkdir -p $desired_job_file_location_master"

        #change ownership of created folder
        ssh root@$MASTER_IP "chown hduser:hadoop $desired_job_file_location_master"
        ssh root@$MASTER_IP "chown hduser:hadoop $desired_input_file_location_master"
        ssh root@$MASTER_IP "chown hduser:hadoop $desired_output_file_location_master"

        #copy jar file to master machine
        scp $jar_file_path hduser@$MASTER_IP:$desired_job_file_location_master/$jar_name

        #copy test file to master machine
     #Commented for uploading data from another machine to hadoop
	 scp $input_file_path hduser@$MASTER_IP:$desired_input_file_location_master/$file_name
    
#	 for ip in "${ips[@]}"
#        do

#	done

}

data_publishing(){

	get_process_id DATA_PUBLISHING
        process_instance_dbwrite $process_id $job_id
        process_instance_detail_dbwrite $process_instance_id

        #get  time for data_publishing
        time_publishing=$(date +'%Y:%m:%d %H:%M:%S')

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update process_instance_detail set  status='started', execution_start_time='$time_publishing' where process_instance_detail_id='$process_instance_detail_id';"


        #start_upload=$(date +%s.%N)     
        upload_to_hdfs $desired_input_file_location_master/$file_name ${desired_input_file_location_hdfs}/${file_name}

         #get  time for data_publishing finished
        time_publishing_finished=$(date +'%Y:%m:%d %H:%M:%S')

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update process_instance_detail set  status='finished', execution_end_time='$time_publishing_finished' where process_instance_detail_id='$process_instance_detail_id';"



}

show_data(){

        start_ls=$(date +%s.%N)
        view_data_of_hdfs ${desired_input_file_location_hdfs}/${file_name}
        time_ls=$(echo "$(date +%s.%N) - $start_ls"| bc)
        echo -e "\nExecution time for showing files in hdfs: $time_ls seconds" 


}

job_execution(){

	get_process_id JOB_EXECUTION
        process_instance_dbwrite $process_id $job_id
        process_instance_detail_dbwrite $process_instance_id

        #get  time for data_publishing
        time_execution=$(date +'%Y:%m:%d %H:%M:%S')

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update process_instance_detail set  status='started', execution_start_time='$time_execution' where process_instance_detail_id='$process_instance_detail_id';"


        run_job $desired_job_file_location_master/$jar_name $class_name $desired_input_file_location_hdfs/${file_name} $desired_output_file_location_hdfs/$output_file_name


        #get  time for data_execution finished
        time_execution_finished=$(date +'%Y:%m:%d %H:%M:%S')

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update process_instance_detail set  status='finished', execution_end_time='$time_execution_finished' where process_instance_detail_id='$process_instance_detail_id';"


}

transfer_from_hdfs(){

	start_download_from_hdfs=$(date +%s.%N)
        download_from_hdfs $desired_output_file_location_hdfs/$output_file_name $desired_output_file_location_master/$output_file_name
        time_download_from_hdfs=$(echo "$(date +%s.%N) - $start_download_from_hdfs" | bc)
        echo -e "\nExecution time for downloading file from hdfs to local:  $time_download_from_hdfs seconds" 


}

sort_compare_output(){

 #sort output file
        ssh root@$MASTER_IP "sort -o $desired_output_file_location_master/$output_file_name $desired_output_file_location_master/$output_file_name"


        if [ "$expected_result_file_path" != "" ]
       then
                #compare expected result with actual result
                diff_in_result=$(ssh root@$MASTER_IP "diff $desired_expected_result_location_master/$expected_result_file $desired_output_file_location_master/$output_file_name")

                if [ !"$diff_in_result" ]
                then
                        echo -e "\nNo difference in actual file and expected file"
                else
                        echo -e "\nThere is difference in actual file and expected file, please check it manually."
                fi

        fi


}

delete_hdfs_file(){

        get_process_id FILE_DELETE
        process_instance_dbwrite $process_id $job_id
        process_instance_detail_dbwrite $process_instance_id

        #get  time for data_delete
        time_delete=$(date +'%Y:%m:%d %H:%M:%S')

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update process_instance_detail set  status='started', execution_start_time='$time_delete' where process_instance_detail_id='$process_instance_detail_id';"

	delete_hdfs $delete_hdfs_file_name


        #get  time for data_execution finished
        time_delete_finished=$(date +'%Y:%m:%d %H:%M:%S')

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update process_instance_detail set  status='finished', execution_end_time='$time_delete_finished' where process_instance_detail_id='$process_instance_detail_id';"



}

download_hadoop(){
	
	#create dir if not exist
	mkdir -p chef/src/cookbooks/download_hadoop/files/default

	#Download hadoop	
	wget  "https://archive.apache.org/dist/hadoop/core/hadoop-2.7.2/hadoop-2.7.2.tar.gz"

	#move zookeeper to required position
	mv hadoop-2.7.2.tar.gz chef/src/cookbooks/download_hadoop/files/default

}

download_spark()
{

        #create dir if not exist
        mkdir -p chef/src/cookbooks/download_spark/files/default/

        #Download spark     
         wget http://www-eu.apache.org/dist/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz

        #move zookeeper to required position
        mv  spark-2.2.0-bin-hadoop2.7.tgz chef/src/cookbooks/download_spark/files/default/

}


remove_known_hosts()
{
        ips=("${!1}")
        echo "${ips[@]}"
        j=0
        for i in "${ips[@]}"
        do
                ssh-keygen -R $i
        done
}

start_spark_all()
{
        ips=("${!1}")
        ssh hduser@${ips[0]} "sh /usr/local/spark-2.2.0-bin-hadoop2.7/sbin/start-all.sh"
}

copy_ips_to_slaves()
{
        ips=("${!1}")
        echo "${ips[@]}"
        rm -f ip.txt
        j=0 
        for i in "${ips[@]}"
        do
                echo $i >> ip.txt
        done

        for i in "${ips[@]}"
        do
                echo $i

                j=`expr $j + 1`


                chmod 777 ip.txt

                #copy ip file to every node
                cat ip.txt | ssh hduser@$i "cat >>/usr/local/spark-2.2.0-bin-hadoop2.7/conf/slaves"
                sleep 1
                #scp ip_file.txt  root@$i:/etc/ip_file.txt
                #vagrant ssh hadoop-$j -c '/etc/ip_file.txt >> /etc/hosts'

                if [ $j -eq 1  ]
                then
                        SPARK_MASTER_HOST=$i
                        ssh hduser@$i "echo "SPARK_MASTER_HOST="$i >> /usr/local/spark-2.2.0-bin-hadoop2.7/conf/spark-env.sh"

                        ssh hduser@$i "echo "SPARK_LOCAL_IP="$i >> /usr/local/spark-2.2.0-bin-hadoop2.7/conf/spark-env.sh"
                                
                        ssh hduser@$i "echo "SPARK_WORKER_PORT="9090 >> /usr/local/spark-2.2.0-bin-hadoop2.7/conf/spark-env.sh"
                                
                        ssh hduser@$i "echo "SPARK_MASTER_PORT="9091 >> /usr/local/spark-2.2.0-bin-hadoop2.7/conf/spark-env.sh"
                        sleep 1
                else
                        ssh hduser@$i "echo "SPARK_LOCAL_IP="$i >> /usr/local/spark-2.2.0-bin-hadoop2.7/conf/spark-env.sh"
                        ssh hduser@$i "echo "SPARK_WORKER_PORT="9090 >> /usr/local/spark-2.2.0-bin-hadoop2.7/conf/spark-env.sh"

                fi

	done
}
