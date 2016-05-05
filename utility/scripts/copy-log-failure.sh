#!/bin/bash
jobuuid=$1
echo "copying error logs..."
cp ./$jobuuid/*.err /root/hungryhippos/job/logs/error/$jobuuid/
echo "done."