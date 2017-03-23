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
if [[ ( -z "$1") || ( -z "$2" ) || (-z "$3") || || (-z "$4")  || (-z "$5:wq")  ]];
then
	echo "provide client-Configuration.xml file path, jar location,implementation Class,input dir and output dir "
	exit
fi
java  com.talentica.hungryHippos.job.main.JobOrchestrator $1 $2 $3 $4 $5 > ../logs/job-man.out 2>../logs/job-man.err &
