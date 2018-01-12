#
# Cookbook Name:: download_hadoop
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

cookbook_file "/usr/local/hadoop-2.7.2.tar.gz" do
source "hadoop-2.7.2.tar.gz"
mode "777"
end

execute "extract hadoop source" do
  command "sudo tar -xzf /usr/local/hadoop-2.7.2.tar.gz"
end

execute "copy extracted file from / to /usr/local/hadoop" do
  command "mv  /hadoop-2.7.2 /usr/local/hadoop"
end

execute "change ownership of hadoop folder to hduser" do
  command "chown hduser:hadoop -R /usr/local/hadoop"
end
