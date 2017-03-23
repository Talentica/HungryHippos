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

ips=($(awk -F ':' '{print $1}' ip_file.txt))

print_used_space(){

rm -f data.txt
echo "enter cluster input dir"
read cluster_input_dir
for ip in "${ips[@]}"
do
    echo $ip >> data.txt
    ssh hhuser@$ip "du -sh /home/hhuser/hh/filesystem/$cluster_input_dir/data_" >> data.txt
done

echo "nodes disk usage for $cluster_input_dir details are stored in data.txt"
}




