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

export NODENUM
export ZOOKEEPERNUM
export PROVIDER
export NAME
export PRIVATE_KEY_PATH
export TOKEN
export IMAGE
export REGION
export RAM
export SSH_KEY_NAME

file="./vagrant.properties"

NODENUM=$(get_vagrant_values_associated_with_key $file NODENUM)
#echo node $NODENUM
ZOOKEEPERNUM=$(get_vagrant_values_associated_with_key $file ZOOKEEPERNUM)
#echo zoo $ZOOKEEPERNUM
PROVIDER=$(get_vagrant_values_associated_with_key $file PROVIDER)
#echo $PROVIDER
NAME=$(get_vagrant_values_associated_with_key $file NAME)
#echo $NAME
PRIVATE_KEY_PATH=$(get_vagrant_values_associated_with_key $file PRIVATE_KEY_PATH)
#echo $PRIVATE_KEY_PATH
TOKEN=$(get_vagrant_values_associated_with_key $file TOKEN)
#echo $TOKEN
IMAGE=$(get_vagrant_values_associated_with_key $file IMAGE)
#echo $IMAGE
REGION=$(get_vagrant_values_associated_with_key $file REGION)
#echo $REGION
RAM=$(get_vagrant_values_associated_with_key $file RAM)
#echo $RAM
SSH_KEY_NAME=$(get_vagrant_values_associated_with_key $file SSH_KEY_NAME)
#echo $SSH_KEY_NAME

