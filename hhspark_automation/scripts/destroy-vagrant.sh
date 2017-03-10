#!/bin/bash

source assign-global-variables.sh

vagrant destroy -f

rm -f ip_file*.txt
rm -rf ../distr
