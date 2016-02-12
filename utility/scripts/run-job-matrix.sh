#!/bin/bash

sh shut-down-all-nodes.sh
sh start-all-nodes-job-execution.sh
sh start-job-manager.sh $1
