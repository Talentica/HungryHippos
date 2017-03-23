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

class_name
 ./spark-submit --class com.talentica.hungryHippos.rdd.main.SumJobWithShuffle --master spark://192.241.154.239:9091 --jars /home/hhuser/distr/lib_client/sharding-0.7.0.jar /home/hhuser/distr/lib_client/spark-0.7.0.jar spark://192.241.154.239:9091 HHJobSumWithShuffle /dir/input3 /home/hhuser/distr/config/client-config.xml /dir/sumout3 >../logs/spark_sum3.out 2>../logs/spark_sum3.err &
