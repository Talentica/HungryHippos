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
        #remove " from variable"
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

