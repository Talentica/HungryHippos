#!/bin/bash
jobuuid=$1
ngnixip=192.241.248.197
echo "copying success logs..."
scp -o StrictHostKeyChecking=no ./$jobuuid/*.out root@$ngnixip:/root/hungryhippos/job/logs/success/$jobuuid/
echo "done."