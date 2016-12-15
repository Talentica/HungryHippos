#!/bin/bash 

start_vagrantfile()
{

	no_of_nodes=$1
	no_of_zookeeper=$2
	provider=$3

	#Start vagrant file

	if [ "$provider" == "digital_ocean"  ]
        then	
		NODENUM=$no_of_nodes ZOOKEEPERNUM=$no_of_zookeeper PROVIDER=$provider  vagrant up --provider=digital_ocean
	elif [ "$provider" == "virtual_box"  ]
	then
		NODENUM=$no_of_nodes ZOOKEEPERNUM=$no_of_zookeeper PROVIDER=$provider  vagrant up
	fi

	#NODENUM=$no_of_nodes ZOOKEEPERNUM=$no_of_zookeeper  vagrant up

	sleep 10
}


file_processing_to_getIP()
{
	#Copy original file to tmp file to perform file operatiob
	cp ip_file.txt ip_file_tmp.txt


	#replace tab with colon in tmp file
	sed -i 's/	/:/g' ip_file_tmp.txt

	#Sort file  as we want HadoopMaster to come at first line
	sort  -t':' -k2 -V  ip_file_tmp.txt -o ip_file_tmp.txt

}


File_processing_to_get_zookeeper_ip(){

        #Copy original file to tmp file to perform file operatiob
        cp ip_file.txt ip_file_zookeeper.txt


        #replace tab with colon in tmp file
        sed -i 's/	/:/g' ip_file_zookeeper.txt

        #Sort file  as we want HadoopMaster to come at first line
        sort  -t':' -k2 ip_file_zookeeper.txt -o ip_file_zookeeper.txt

	sed -i '/Zookeeper/!d' ip_file_zookeeper.txt      

	#Sort file  as we want HadoopMaster to come at first line
        sort  -t':' -k2 ip_file_zookeeper.txt -o ip_file_zookeeper.txt


}

create_zookeeperip_string(){

	zookeeper_ips=("${!1}")

	i=0
	for zookeeper_ip in "${zookeeper_ips[@]}"
	do
		if [ $i -eq "0" ]
		then
		zookeeperip_string="$zookeeper_ip:2181"
		else
		zookeeperip_string="$zookeeperip_string,$zookeeper_ip:2181"
		fi
		
		i=`expr $i + 1`

	done	


}

update_client-config(){

	zookeeperip_string_config="\<tns\:servers\>$zookeeperip_string\<\/tns\:servers\>"


	#get line no having <tns:servers> in file
	line_no=$(grep -n  '<tns:servers>' distr/config/client-config.xml | awk -F  ":" '{print $1}' )

	#delete existing line containing zookeeper string
	sed -i "${line_no}d" distr/config/client-config.xml

	#add new zookeeper string
	sed -i "${line_no}i $zookeeperip_string_config" distr/config/client-config.xml
}

update_cluster-config(){

	i=0
	for ip in "${ips[@]}"
	do
		echo "	<tns:node>" >> tmp_cluster-config.xml
		echo "		<tns:identifier>$i</tns:identifier>" >> tmp_cluster-config.xml
		echo "		<tns:name>node$i</tns:name>" >> tmp_cluster-config.xml
		echo "		<tns:ip>$ip</tns:ip>" >> tmp_cluster-config.xml
		echo "		<tns:port>2324</tns:port>" >> tmp_cluster-config.xml
		echo "	</tns:node>" >> tmp_cluster-config.xml

		i=`expr $i + 1`
	done

	#adding tmp file to original file
	sed -i -e '/xsi\:schemaLocation/r tmp_cluster-config.xml' distr/config/cluster-config.xml 

	rm tmp_cluster-config.xml

}

run_script_on_random_node(){
	zookeeperip_string=$1
	ip=$2
	pathof_config_folder="/home/hhuser/distr/config"
	ssh hhuser@$ip "cd /home/hhuser/distr/bin && ./coordination.sh $pathof_config_folder"
	ssh hhuser@$ip "cd /home/hhuser/distr/bin && ./torrent-tracker.sh $zookeeperip_string $ip"

}


run_all_scripts()
{
	zookeeperip_string=$1
	ip=$2
	ssh hhuser@$ip "cd /home/hhuser/distr/bin && ./start-all.sh $zookeeperip_string $ip"

}

read_json_hungryhippos(){

        i=$1

         #read values from json file
  	lib_client=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].lib_client')
	filepath_sharding_jar=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].filepath_sharding_jar')
         sharding_class=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i" '.jobs['$i'].sharding_class')
         filepath_client_config=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].filepath_client_config')
         folderpath_config=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].folderpath_config')
         filepath_sharding_output_log=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].filepath_sharding_output_log')
         filepath_sharding_error_log=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].filepath_sharding_error_log')
         filepath_datapublisher_jar=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].filepath_datapublisher_jar')
         datapublisher_class=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].datapublisher_class')
         input_filepath=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].input_filepath')
	 sampling_filepath=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].sampling_filepath')
         distributed_input_filepath=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].distributed_input_filepath')
         filepath_datapublisher_output_log=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].filepath_datapublisher_output_log')
         filGepath_datapublisher_error_log=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].filepath_datapublisher_error_log')
         joborchestrator_class=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].joborchestrator_class')
         job_filepath=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].job_filepath')
         job_class=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i']  .job_class')
         distributed_output_filepath=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].distributed_output_filepath')
         filepath_orchestrator_output_log=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].filepath_orchestrator_output_log')
         filepath_orchestrator_error_log=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].filepath_orchestrator_error_log')
        local_path_to_copy_file_from_hhfs=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].local_path_to_copy_file_from_hhfs') 
	delete_file=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].delete_file')
	file_to_copy_to_local=$(cat hungryhippos_operations_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].file_to_copy_to_local')
	mysql_server=$(cat hungryhippos_operations_conf.json  |jq '.mysql_server')
	mysql_username=$(cat hungryhippos_operations_conf.json  |jq '.mysql_username')
	mysql_password=$(cat hungryhippos_operations_conf.json  |jq '.mysql_password')

	

        #remove " from variable
	 lib_client=$(echo "$lib_client" | tr -d '"')
	 filepath_sharding_jar=$(echo "$filepath_sharding_jar" | tr -d '"')
	 sharding_class=$(echo "$sharding_class" | tr -d '"')
	 filepath_client_config=$(echo "$filepath_client_config" | tr -d '"')
	 folderpath_config=$(echo "$folderpath_config" | tr -d '"')
	 filepath_sharding_output_log=$(echo "$filepath_sharding_output_log" | tr -d '"')
	 filepath_sharding_error_log=$(echo "$filepath_sharding_error_log" | tr -d '"')
	 filepath_datapublisher_jar=$(echo "$filepath_datapublisher_jar" | tr -d '"')
	 datapublisher_class=$(echo "$datapublisher_class" | tr -d '"')
	 input_filepath=$(echo "$input_filepath" | tr -d '"')
	sampling_filepath=$(echo "$sampling_filepath" | tr -d '"')
	 distributed_input_filepath=$(echo "$distributed_input_filepath" | tr -d '"')
	 filepath_datapublisher_output_log=$(echo "$filepath_datapublisher_output_log" | tr -d '"')
	 filepath_datapublisher_error_log=$(echo "$filepath_datapublisher_error_log" | tr -d '"')
	 joborchestrator_class=$(echo "$joborchestrator_class" | tr -d '"')
	 job_filepath=$(echo "$job_filepath" | tr -d '"')
	 job_class=$(echo "$job_class" | tr -d '"')
	 distributed_output_filepath=$(echo "$distributed_output_filepath" | tr -d '"')
	 filepath_orchestrator_output_log=$(echo "$filepath_orchestrator_output_log" | tr -d '"')
	 filepath_orchestrator_error_log=$(echo "$filepath_orchestrator_error_log" | tr -d '"')
        local_path_to_copy_file_from_hhfs=$(echo "$local_path_to_copy_file_from_hhfs" | tr -d '"')
	delete_file=$(echo "$delete_file" | tr -d '"')
	file_to_copy_to_local=$(echo "$file_to_copy_to_local" | tr -d '"')
	mysql_server=$(echo "$mysql_server" | tr -d '"')
	mysql_username=$(echo "$mysql_username" | tr -d '"')
	mysql_password=$(echo "$mysql_password" | tr -d '"')


}

start_sharding(){

        get_process_id SHARDING
        process_instance_dbwrite $process_id $job_id
        process_instance_detail_dbwrite $process_instance_id
	
	#get  time for data_publishing
        time_sharding=$(date +'%Y:%m:%d %H:%M:%S')

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update process_instance_detail set  status='started', execution_start_time='$time_sharding' where process_instance_detail_id='$process_instance_detail_id';"
	

	#start_sharding
	java -cp $filepath_sharding_jar $sharding_class $filepath_client_config $folderpath_config > $filepath_sharding_output_log 2> $filepath_sharding_error_log

	echo "Sharding completed"

	#get  time for data_publishing finished
        time_sharding_finished=$(date +'%Y:%m:%d %H:%M:%S')

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update process_instance_detail set  status='finished', execution_end_time='$time_sharding_finished' where process_instance_detail_id='$process_instance_detail_id';"


}

start_publishing(){

	get_process_id DATA_PUBLISHING
	process_instance_dbwrite $process_id $job_id
        process_instance_detail_dbwrite $process_instance_id

        #get  time for data_publishing
        time_publishing=$(date +'%Y:%m:%d %H:%M:%S')

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update process_instance_detail set  status='started', execution_start_time='$time_publishing' where process_instance_detail_id='$process_instance_detail_id';"



	java -cp $filepath_datapublisher_jar $datapublisher_class $filepath_client_config  $input_filepath  $distributed_input_filepath > $filepath_datapublisher_output_log 2> $filepath_datapublisher_error_log

	#get  time for data_publishing finished
        time_publishing_finished=$(date +'%Y:%m:%d %H:%M:%S')

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update process_instance_detail set  status='finished', execution_end_time='$time_publishing_finished' where process_instance_detail_id='$process_instance_detail_id';"
	

	echo "Data publishing completed"
}

start_execution(){
	
	get_process_id JOB_EXECUTION
        process_instance_dbwrite $process_id $job_id
        process_instance_detail_dbwrite $process_instance_id

        #get  time for data_publishing
        time_execution=$(date +'%Y:%m:%d %H:%M:%S')

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update process_instance_detail set  status='started', execution_start_time='$time_execution' where process_instance_detail_id='$process_instance_detail_id';"
	

	export CLASSPATH="$lib_client/*"
	java $joborchestrator_class $filepath_client_config $job_filepath $job_class $distributed_input_filepath $distributed_output_filepath > $filepath_orchestrator_output_log  2> $filepath_orchestrator_error_log

	 #get  time for data_execution finished
        time_execution_finished=$(date +'%Y:%m:%d %H:%M:%S')

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update process_instance_detail set  status='finished', execution_end_time='$time_execution_finished' where process_instance_detail_id='$process_instance_detail_id';"

	
	echo "job execution completed"
}

start_sampling(){

        get_process_id SAMPLING
        process_instance_dbwrite $process_id $job_id
        process_instance_detail_dbwrite $process_instance_id

        #get  time for data_sampling
        time_sampling=$(date +'%Y:%m:%d %H:%M:%S')

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update process_instance_detail set  status='started', execution_start_time='$time_sampling' where process_instance_detail_id='$process_instance_detail_id';"

	python $lib_client/sampling.py $input_filepath  $sampling_filepath

         #get  time for data_sampling finished
        time_sampling_finished=$(date +'%Y:%m:%d %H:%M:%S')

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update process_instance_detail set  status='finished', execution_end_time='$time_sampling_finished' where process_instance_detail_id='$process_instance_detail_id';"


        echo "job sampling completed"
}

get_hhfs_file_size_output(){

	java  -cp $lib_client/file-system.jar com.talentica.hungryhippos.filesystem.main.HungryHipposFileSystemMain $lib_client/file-system-commands.sh $filepath_client_config du $distributed_output_filepath > filesize.txt
	
	file_size_output=$(awk -F ':'  '/fileSize is /{ print $2 }' filesize.txt)

	file_size_output=$(echo $file_size_output | cut -d' ' -f1)
        NUM=$file_size_output
        file_size_output=$(echo "scale=2;$NUM/1024" | bc)
	
	#rm filesize.txt

}

hh_job_input_dbwrite(){

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "INSERT INTO job_input (job_id,data_location,data_size_in_kbs,job_matrix_class,sharding_dimensions,data_type_configuration) VALUES ('$1', '$2','$3','$4','$5','$6');"
}

hh_job_output_dbwrite(){

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "INSERT INTO job_output (job_id,data_location,data_size_in_kbs) VALUES ('$1', '$2','$3');"
}

get_sharding_dimension(){

sharding_dimension_string=$(sed -n 's/<tns:sharding-dimensions>\(.*\)<\/tns:sharding-dimensions>/\1/p' $folderpath_config/sharding-client-config.xml | tr -d "	")

}


get_data_type_configuration(){

sed -ne 's/<tns:data-type>\(.*\)<\/tns:data-type>/\1/p;s/<tns:size>\(.*\)<\/tns:size>/\1/p' $folderpath_config/sharding-client-config.xml |tr -d "	" > data_type_configuration.txt

sed 'N;s/\n/-/' data_type_configuration.txt > data_type_configuration_temp.txt


xargs -a data_type_configuration_temp.txt | sed 's/ /,/g' > data_type_configuration.txt

data_type_configuration=$(cat data_type_configuration.txt)

rm data_type_configuration.txt data_type_configuration_temp.txt

}



update_sharding-client-config(){

	#update sampling file path 
	sampling_filepath_string_config="\<tns\:sample-file-path\>$sampling_filepath\<\/tns\:sample-file-path\>"

        #get line no having <tns:sample-file-path> in file
        line_no=$(grep -n  '<tns:sample-file-path>' $folderpath_config/sharding-client-config.xml | awk -F  ":" '{print $1}' )

        #delete existing line containing sampling_file_path  string
        sed -i "${line_no}d" $folderpath_config/sharding-client-config.xml

        #add new sampling_file_path string
        sed -i "${line_no}i $sampling_filepath_string_config" $folderpath_config/sharding-client-config.xml


	#update distributed file path

	distributed_filepath_string_config="\<tns\:distributed-file-path\>$distributed_input_filepath\<\/tns\:distributed-file-path\>"
        #get line no having <tns:distributed-file-path> in file
        line_no=$(grep -n  '<tns:distributed-file-path>' $folderpath_config/sharding-client-config.xml | awk -F  ":" '{print $1}' )

        #delete existing line containing distributed_file_path  string
        sed -i "${line_no}d" $folderpath_config/sharding-client-config.xml

        #add new distributed_file_path string
        sed -i "${line_no}i $distributed_filepath_string_config" $folderpath_config/sharding-client-config.xml

        #update jar-file-path of job

        jar_filepath_string_config="\<tns\:jar-file-path\>$job_filepath\<\/tns\:jar-file-path\>"

        #get line no having <tns:jar-file-path> in file
        line_no=$(grep -n  '<tns:jar-file-path>' $folderpath_config/sharding-client-config.xml | awk -F  ":" '{print $1}' )

        #delete existing line containing jar-file-path  string
        sed -i "${line_no}d" $folderpath_config/sharding-client-config.xml

        #add new distributed_file_path string
        sed -i "${line_no}i $jar_filepath_string_config" $folderpath_config/sharding-client-config.xml

}


delete_hhfs_file(){

	echo "Deleting file $delete_file"

	get_process_id FILE_DELETE
        process_instance_dbwrite $process_id $job_id
        process_instance_detail_dbwrite $process_instance_id

	#get  time for deleting hhfs file
        time_deleting=$(date +'%Y:%m:%d %H:%M:%S')

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update process_instance_detail set  status='started', execution_start_time='$time_deleting' where process_instance_detail_id='$process_instance_detail_id';"


	java  -cp $lib_client/file-system.jar com.talentica.hungryhippos.filesystem.main.HungryHipposFileSystemMain $lib_client/file-system-commands.sh  $filepath_client_config deleteall $delete_file	

	  #get end time for deleting hhfs file
        time_deleting_finished=$(date +'%Y:%m:%d %H:%M:%S')

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update process_instance_detail set  status='finished', execution_end_time='$time_deleting_finished' where process_instance_detail_id='$process_instance_detail_id';"


        echo "File deleting completed"


}

copy_to_local_from_hhfs(){

	echo "Copying file $file_to_copy_to_local to $local_path_to_copy_file_from_hhfs"

	get_process_id OUTPUT_TRANSFER
        process_instance_dbwrite $process_id $job_id
        process_instance_detail_dbwrite $process_instance_id

        #get  time for copying hhfs file
        time_copying=$(date +'%Y:%m:%d %H:%M:%S')

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update process_instance_detail set  status='started', execution_start_time='$time_copying' where process_instance_detail_id='$process_instance_detail_id';"

	java  -cp $lib_client/file-system.jar com.talentica.hungryhippos.filesystem.main.HungryHipposFileSystemMain $lib_client/file-system-commands.sh $filepath_client_config download $file_to_copy_to_local $local_path_to_copy_file_from_hhfs

	#get end time for deleting hhfs file
        time_copying_finished=$(date +'%Y:%m:%d %H:%M:%S')

        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update process_instance_detail set  status='finished', execution_end_time='$time_copying_finished' where process_instance_detail_id='$process_instance_detail_id';"


        echo "File copy completed"
	

}

download_zookeeper()
{
	
	#create dir if not exist
	mkdir -p chef/src/cookbooks/download_zookeeper/files/default/

	#Download zookeeper	
	wget  "http://www-us.apache.org/dist/zookeeper/zookeeper-3.5.1-alpha/zookeeper-3.5.1-alpha.tar.gz"

	#move zookeeper to required position
	mv zookeeper-3.5.1-alpha.tar.gz chef/src/cookbooks/download_zookeeper/files/default/ 

}

download_spark()
{

        #create dir if not exist
        mkdir -p chef/src/cookbooks/download_spark/files/default/

        #Download spark     
        wget http://d3kbcqa49mib13.cloudfront.net/spark-2.0.2-bin-hadoop2.7.tgz

        #move zookeeper to required position
        mv  spark-2.0.2-bin-hadoop2.7.tgz chef/src/cookbooks/download_spark/files/default/

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
	        cat ip.txt | ssh hhuser@$i "cat >>/home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/slaves"
	        sleep 1
	        #scp ip_file.txt  root@$i:/etc/ip_file.txt
	        #vagrant ssh hadoop-$j -c '/etc/ip_file.txt >> /etc/hosts'

	        if [ $j -eq 1  ]   
	        then
	        	SPARK_MASTER_HOST=$i
	        	ssh hhuser@$i "echo "SPARK_MASTER_HOST="$i >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-env.sh"
	        	
	        	ssh hhuser@$i "echo "SPARK_LOCAL_IP="$i >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-env.sh"
				
			ssh hhuser@$i "echo "SPARK_WORKER_PORT="9090 >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-env.sh"
				
			ssh hhuser@$i "echo "SPARK_MASTER_PORT="9091 >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-env.sh"
			sleep 1   
        	else
        		ssh hhuser@$i "echo "SPARK_LOCAL_IP="$i >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-env.sh"
        		ssh hhuser@$i "echo "SPARK_WORKER_PORT="9090 >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-env.sh"
                
	        fi
	        
	        

	done
    

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
	ssh hhuser@${ips[0]} "sh /home/hhuser/spark-2.0.2-bin-hadoop2.7/sbin/start-all.sh"
}

