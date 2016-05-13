#!/bin/bash

echo "Starting tester web app on $1"
ssh -o StrictHostKeyChecking=no root@$1 "cd hungryhippos/tester-web;java -XX:HeapDumpPath=./ -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./ -jar hungryhippos-tester-web.jar > ./system.out 2>./system.err &"

