#
# Cookbook Name:: cluster_mode_zookeeper
# Recipe:: default
#
# Copyright 2016, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#

#Create zookeeper dir
directory '/var/lib/zookeeper' do
  owner 'hhuser'
  group 'hungryhippos'
  mode '0755'
  action :create
end

#Create my id file
file '/var/lib/zookeeper/myid' do
  mode '0755'
  owner 'hhuser'
  group 'hungryhippos'
end

#Writing server id to myid file. Here we consider '1' in hungryhippos-1 as server id
bash 'writing_server_id' do
  user 'hhuser'
  cwd '/var/lib/zookeeper'
  code <<-EOH
  IFS='-' read -a id <<<  `hostname`
  echo ${id[${#id[@]}-1]}  >> myid
  EOH
end

#creating file in zookeeper installation folder
file '/home/hhuser/zookeeper-3.5.1-alpha/conf/zoo.cfg' do
  mode '0755'
  owner 'hhuser'
  group 'hungryhippos'
end

#adding default lines to zoo.conf file
bash 'write_to_zoo.conf' do
  user 'hhuser'
  cwd '/home/hhuser/zookeeper-3.5.1-alpha/conf'
  code <<-EOH
  echo tickTime=2000 > zoo.cfg
  echo dataDir=/var/lib/zookeeper/ >> zoo.cfg
  echo clientPort=2181 >> zoo.cfg
  echo initLimit=5 >> zoo.cfg
  echo syncLimit=2 >> zoo.cfg
  EOH
end

