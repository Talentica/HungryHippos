#
# Cookbook Name:: hadoop_master_conf_setup
# Recipe:: default
#
# Copyright 2016, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#
directory '/usr/local/hadoop_tmp/' do
  owner 'hduser'
  group 'hadoop'
  mode '0777'
  action :create
end

directory '/usr/local/hadoop_tmp/hdfs' do
  owner 'hduser'
  group 'hadoop'
  mode '0777'
  action :create
end


directory '/usr/local/hadoop_tmp/hdfs/namenode' do
  owner 'hduser'
  group 'hadoop'
  mode '0777'
  action :create
end

##Copying hadoop test file to master node 
#cookbook_file "/usr/local/hadoop/test.txt" do
#  user "hduser"
#  group "hadoop" 
#  source "pg20417.txt"
# end

##Copying hadoop expected test result file to master node 
#cookbook_file "/usr/local/hadoop/expected_test_result.txt" do
#  user "hduser"
#  group "hadoop"
#  source "expected_test_result.txt"
# end



##Copying hadoop zip  file to master node
#cookbook_file "/usr/local/hadoop/Hadoop-WordCount.zip" do
#   source "Hadoop-WordCount.zip"
# end

#execute "Installing Unzip" do
 # user "root"
 # command "sudo apt-get install unzip"
#end

#execute "Unzipping wordcount example" do
#  user "hduser"
#  group "hadoop"
#  command "unzip /usr/local/hadoop/Hadoop-WordCount.zip -d /usr/local/hadoop/"
#end

