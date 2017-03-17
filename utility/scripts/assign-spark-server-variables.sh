#!/bin/bash

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
