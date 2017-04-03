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

for i in `cat node_list.txt`
do

ip=`echo $i|head -1| awk -F"," '{print $1}'`
initial_pwd=`echo $i|head -1| awk -F"," '{print $2}'`

sed -e "s/node_ip/$ip/g" -e "s/node_initial_pwd/$initial_pwd/g" initial-connect-to-server.sh > initial-connect-to-$ip.sh
chmod 755 initial-connect-to-$ip.sh
./initial-connect-to-$ip.sh
done
