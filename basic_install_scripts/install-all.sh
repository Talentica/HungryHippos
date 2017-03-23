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

echo "installation started " 
date 
#installing java
sh ./install-java.sh

#install ruby
sh ./install-ruby.sh

#install chef
sh ./install-chef.sh

#install virtual-box
sh ./install-virtual-box.sh

#install vagrant 
sh ./install-vagrant.sh

#install bc
#sh ./install-bc.sh

#install jq	
sh ./install-jq.sh

rm -f vagrant_1.8.5_x86_64.deb 
#install mysql password if prompted type "root".
#sh ./install-mysql.sh

sh ./comment.sh

echo "finished installing all softwares "
date
