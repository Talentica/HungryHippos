#!/bin/bash
jobUuid=$1
sh shut-down-job-manager.sh $jobUuid
sh cleanup-job-manager.sh $jobUuid
echo 'Copying new build'
sh copy-file-to-job-manager.sh ../lib/job-manager.jar $jobUuid
echo 'Copying test jobs jar'
sh copy-file-to-job-manager.sh ../lib/$jobUuid/test-jobs.jar $jobUuid