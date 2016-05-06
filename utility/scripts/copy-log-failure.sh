#!/bin/bash
jobuuid=$1
ngnixip=192.241.248.197
echo "copying error logs..."
scp -o StrictHostKeyChecking=no ./$jobuuid/*.err root@$ngnixip:/root/hungryhippos/job/logs/error/$jobuuid/
echo "done."