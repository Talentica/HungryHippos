#!/bin/bash

sh shut-down-all-nodes.sh
sh clear-all-nodes-buffers.sh
sh start-job-manager.sh $1
sh start-all-nodes-job-execution.sh

