#!/bin/bash

sh shut-down-manager.sh
sh shut-down-all-nodes.sh
sh shut-down-zk-server.sh
sh cleanup-all-nodes-data.sh
sh cleanup-manager.sh
sh start-zk-server.sh
sh start-manager.sh $1 &
