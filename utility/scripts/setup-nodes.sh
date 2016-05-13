jobUuid=$1
sh shut-down-all-nodes.sh $jobUuid
sh cleanup-all-nodes-data.sh $jobUuid
sh copy-test-jobs-jar-all-nodes.sh $jobUuid
sh copy-node-jar-to-all-nodes.sh $jobUuid
sh create-node-id-files.sh $jobUuid
