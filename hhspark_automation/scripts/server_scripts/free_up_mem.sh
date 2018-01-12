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

minFreeMem=4294967296

mulVal=1
multiplier(){
	mulVal=1
	val=$1
	if [ $val = "kB" -o $val = "KB" ]; then
		mulVal=1024
	elif [ $val = "mB" -o $val = "MB" ]; then
		mulVal=1048576
	elif [ $val = "gB" -o $val = "GB" ]; then
		mulVal=1073741824
	fi
}

freeMemVal=`cat /proc/meminfo | grep "^MemFree:" | awk -F ' ' '{print $2}'`
freeMemType=`cat /proc/meminfo | grep "^MemFree:" | awk -F ' ' '{print $3}'`
multiplier $freeMemType

freeMem=`expr $freeMemVal \* $mulVal `
echo $freeMem
if [ $freeMem -le $minFreeMem ] ; then
	echo Clearing Memory
	sync &&	sudo /sbin/sysctl vm.drop_caches=3
fi
