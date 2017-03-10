#!/bin/bash

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
export STORAGE_NAME

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
STORAGE_NAME=$(get_vagrant_values_associated_with_key $file STORAGE_NAME)

