#!/bin/bash
jobuuid=$1
ngnixip=`cat ../conf/ngnixip`
success_logspath=/root/hungryhippos/job/logs/success

echo "create directory for $jobuuid"
ssh -o StrictHostKeyChecking=no root@$ngnixip "cd $success_logspath;mkdir $jobuuid;"
echo "done."

echo "copying success logs..."
scp -o StrictHostKeyChecking=no ../lib/$jobuuid/*.out root@$ngnixip:$success_logspath/$jobuuid/
echo "done."