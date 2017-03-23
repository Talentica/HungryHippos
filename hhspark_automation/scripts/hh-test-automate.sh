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

