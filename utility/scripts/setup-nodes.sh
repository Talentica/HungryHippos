jobUuid=$1
sh shut-down-all-nodes.sh
sh cleanup-all-nodes-data.sh
sh copy-test-jobs-jar-all-nodes.sh $jobUuid
sh copy-node-jar-to-all-nodes.sh
sh create-node-id-files.sh
