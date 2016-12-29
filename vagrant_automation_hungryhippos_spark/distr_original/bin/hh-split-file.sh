#!/usr/bin/env bash
src_file_path=$1
no_of_chunks=$2
chunk_no=$3
chunk_file_path=$4
split -nl/$chunk_no/$no_of_chunks $src_file_path > $chunk_file_path


