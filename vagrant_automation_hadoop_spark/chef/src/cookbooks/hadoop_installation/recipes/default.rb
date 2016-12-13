#
# Cookbook Name:: hadoop_installation
# Recipe:: default
#
# Copyright 2016, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#
include_recipe "apt"

#to install java
include_recipe "HH_java"
include_recipe "create_group_user"
include_recipe "disable_IPV6"
include_recipe "download_hadoop"
include_recipe "hadoop_conf_files_setup"
#include_recipe "hadoop_ssh_keygen_trans_mastertoslaves"
