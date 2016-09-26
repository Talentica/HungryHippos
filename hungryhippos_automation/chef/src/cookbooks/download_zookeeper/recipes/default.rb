#
# Cookbook Name:: download_zookeeper
# Recipe:: default
#
# Copyright 2016, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#
#package 'hadoop-2.7.2' do
 # action :install
  #package_name 'hadoop-2.7.2'
#end

cookbook_file "/home/hhuser/zookeeper-3.5.1-alpha.tar.gz" do
source "zookeeper-3.5.1-alpha.tar.gz"
mode "777"
end

execute "extract zookeeper source" do
  command "sudo tar -xzvf /home/hhuser/zookeeper-3.5.1-alpha.tar.gz"
end

execute "copy extracted file from / to /usr/local/hadoop" do
 command "mv  /zookeeper-3.5.1-alpha /home/hhuser/zookeeper-3.5.1-alpha"
end

execute "change ownership of hadoop folder to hhuser" do
  command "chown hhuser:hungryhippos -R /home/hhuser/zookeeper-3.5.1-alpha"
end
