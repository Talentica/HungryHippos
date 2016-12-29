#!/bin/bash
ARGS=("$@")
src_folder=$1
tar_file_name=$2
unset ARGS[0]
unset ARGS[1]
cd $src_folder
tar -cf $tar_file_name "${ARGS[@]}";
