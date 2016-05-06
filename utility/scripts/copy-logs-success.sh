#!/bin/bash
jobuuid=$1
echo "copying success logs..."
cp ./$jobuuid/*.out /root/hungryhippos/job/logs/success/$jobuuid/
echo "done."