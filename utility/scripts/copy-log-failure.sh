#!/bin/bash
jobuuid=$1
ngnixip=`cat ../conf/ngnixip`
error_logspath=/root/hungryhippos/job/logs/error

echo "create directory for $jobuuid"
ssh -o StrictHostKeyChecking=no root@$ngnixip "cd $error_logspath;mkdir $jobuuid;"
echo "done."

echo "copying error logs..."
scp -o StrictHostKeyChecking=no ./$jobuuid/*.err root@$ngnixip:$error_logspath/$jobuuid/
echo "done."