#!/bin/bash

source token-reader.sh
export SPARK_WORKER_PORT
export SPARK_MASTER_PORT
export SPARK_EVENT_LOG_ENABLED
export SPARK_EVENT_LOG_DIR
export SPARK_EVENT_LOG_COMPRESS
export SPARK_HISTORY_FS_LOG_DIR


file="./spark.properties"

SPARK_WORKER_PORT=$(get_vagrant_values_associated_with_key $file SPARK_WORKER_PORT)
SPARK_MASTER_PORT=$(get_vagrant_values_associated_with_key $file SPARK_MASTER_PORT)
SPARK_EVENT_LOG_ENABLED=$(get_vagrant_values_associated_with_key $file SPARK_EVENT_LOG_ENABLED)
SPARK_EVENT_LOG_DIR=$(get_vagrant_values_associated_with_key $file SPARK_EVENT_LOG_DIR)
SPARK_EVENT_LOG_COMPRESS=$(get_vagrant_values_associated_with_key $file SPARK_EVENT_LOG_COMPRESS)
SPARK_HISTORY_FS_LOG_DIR=$(get_vagrant_values_associated_with_key $file SPARK_HISTORY_FS_LOG_DIR)

