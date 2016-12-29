#!/bin/bash
ARGS=("$@")
src_folder=$1
tar_file_name=$2
remote_target_folder=$3
ssh_user=$4
ssh_host=$5
unset ARGS[0]
unset ARGS[1]
unset ARGS[2]
unset ARGS[3]
unset ARGS[4]
cd $src_folder
tar -cf $tar_file_name.tar "${ARGS[@]}";
scp -o StrictHostKeyChecking=no $tar_file_name.tar $ssh_user@$ssh_host:$remote_target_folder"/";
rm $tar_file_name.tar;
ssh -o StrictHostKeyChecking=no $ssh_user@$ssh_host "tar -xf $remote_target_folder/$tar_file_name.tar --directory $remote_target_folder";
ssh -o StrictHostKeyChecking=no $ssh_user@$ssh_host "rm $remote_target_folder/$tar_file_name.tar";
