#!/bin/bash
jobUuid=$1
sh shut-down-job-manager.sh
sh cleanup-job-manager.sh
echo 'Copying new build'
sh copy-file-to-job-manager.sh ../lib/job-manager.jar
echo 'Copying test jobs jar'
sh copy-file-to-job-manager.sh ../lib/$jobUuid/test-jobs.jar
