sh shut-down-all-nodes.sh
sh cleanup-all-nodes-data.sh
sh copy-test-jobs-jar-all-nodes.sh
sh copy-config-file-to-all-nodes.sh
sh copy-node-jar-to-all-nodes.sh
sh create-node-id-files.sh
echo 'Copying data file reader utility on all nodes'
sh copy-file-to-all-nodes.sh ./data-file-reader-script.sh
