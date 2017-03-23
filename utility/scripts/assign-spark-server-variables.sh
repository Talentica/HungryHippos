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

source token-reader.sh
export SPARK_EVENT_LOG_ENABLED
export SPARK_EVENT_LOG_DIR
export SPARK_EVENT_LOG_COMPRESS
export SPARK_HISTORY_FS_LOG_DIR
export STORAGE_NAME
export TOKEN

file="./spark.history.server"
STORAGE_NAME=$(get_vagrant_values_associated_with_key $file STORAGE_NAME)
SPARK_EVENT_LOG_ENABLED=$(get_vagrant_values_associated_with_key $file SPARK_EVENT_LOG_ENABLED)
SPARK_EVENT_LOG_DIR=$(get_vagrant_values_associated_with_key $file SPARK_EVENT_LOG_DIR)
SPARK_EVENT_LOG_COMPRESS=$(get_vagrant_values_associated_with_key $file SPARK_EVENT_LOG_COMPRESS)
SPARK_HISTORY_FS_LOG_DIR=$(get_vagrant_values_associated_with_key $file SPARK_HISTORY_FS_LOG_DIR)
TOKEN=$(get_vagrant_values_associated_with_key ../../hhspark_automation/scripts/vagrant.properties TOKEN)
