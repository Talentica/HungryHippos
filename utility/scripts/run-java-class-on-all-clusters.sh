#!/bin/bash
user=$1
node=$2
javavmd=$3

echo "user $user"
echo "java command $javacmd"
echo "logging in to the cluster node $node"
ssh -o StrictHostKeyChecking=no $user@$node "cd hungryhippos; $javacmd"
 






