#!/bin/bash
sh shut-down-job-manager.sh
sh cleanup-job-manager.sh
echo 'Copying new build'
sh copy-file-to-job-manager.sh ../../job-manager/build/libs/job-manager.jar
echo 'Copying test jobs jar'
sh copy-file-to-job-manager.sh ../../test-hungry-hippos/build/libs/test-jobs.jar
