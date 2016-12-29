#!/usr/bin/env bash
srcFolder=$1
srcFileName=$2
cd $srcFolder;
tar -xf $srcFileName;
rm $srcFileName;