#!/bin/bash

source assign-global-variables.sh

base_path=$PRIVATE_KEY_PATH
ssh_key_dir=${base_path%/*}'/'
echo $ssh_key_dir
