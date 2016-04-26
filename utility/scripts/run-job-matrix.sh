#!/bin/bash
jobuuid=$1
sh shut-down-all-nodes.sh $jobuuid
sh clear-all-nodes-buffers.sh $jobuuid
sh start-job-manager.sh $jobuuid
sh start-all-nodes-job-execution.sh $jobuuid

