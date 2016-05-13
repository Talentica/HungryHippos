jobUuid=$1
echo 'Copying test jobs jar on all nodes'
sh copy-file-to-all-nodes.sh ../lib/$jobUuid/test-jobs.jar $jobUuid
