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
jobuuid=$1
ngnixip=`cat ../conf/ngnixip`
success_logspath=/root/hungryhippos/job/logs/success

echo "create directory for $jobuuid"
ssh -o StrictHostKeyChecking=no root@$ngnixip "cd $success_logspath;mkdir $jobuuid;"
echo "done."

echo "copying success logs..."
scp -o StrictHostKeyChecking=no ../lib/$jobuuid/*.out root@$ngnixip:$success_logspath/$jobuuid/
echo "done."