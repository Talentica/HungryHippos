echo 'Copying new build on all nodes'
jobUuid=$1
sh copy-file-to-all-nodes.sh ../lib/node.jar $jobUuid
