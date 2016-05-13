#!/bin/bash
(crontab -l 2>/dev/null; echo "* 23 * * * /usr/bin/python /root/hungryhippos/scripts/python_scripts/cleaning-long-waiting-jobs.py $1 $2 $3") | crontab -
