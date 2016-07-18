#!/bin/bash

node_ip=$1

ssh -o StrictHostKeyChecking=no root@$node_ip 'sudo chef-client'
