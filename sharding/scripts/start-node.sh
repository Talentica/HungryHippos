#!/bin/bash
echo "Starting HungryHippos node $1"
sshpass -p 'Ganesh11' ssh root@$1 "cd hungryhippos;nohup java -jar node-starter.jar &"

