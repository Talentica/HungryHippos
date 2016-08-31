#!/bin/bash
user=$1
node=$2
dir=$3
javacmd=$4

echo "logging in to the cluster node $node"
ssh -o StrictHostKeyChecking=no $user@$node "cd $dir;$javacmd"
 






