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


run_script_on_random_node(){
        zookeeperip_string=$1
        ip=$2
        pathof_config_folder="/home/hhuser/distr/config"
        ssh hhuser@$ip "cd /home/hhuser/distr/bin && ./coordination.sh $pathof_config_folder"

}


run_all_scripts()
{
        zookeeperip_string=$1
        ip=$2
        ssh hhuser@$ip "cd /home/hhuser/distr/bin && ./start-all.sh $zookeeperip_string $ip"

}

