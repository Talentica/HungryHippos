#!/bin/bash

source token-reader.sh
export SPARK_WORKER_PORT
export SPARK_MASTER_PORT


file="./spark.properties"

SPARK_WORKER_PORT=$(get_vagrant_values_associated_with_key $file SPARK_WORKER_PORT)
SPARK_MASTER_PORT=$(get_vagrant_values_associated_with_key $file SPARK_MASTER_PORT)
