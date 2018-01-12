#
# Cookbook Name:: create_group_user
# Recipe:: default
#
# Copyright 2016, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#
group 'hungryhippos' do
  action :create
  group_name 'hungryhippos'
  gid '901'
  append true
end
user 'hhuser' do
  supports :manage_home => true
  uid '1234'
  gid '901'
  home '/home/hhuser'
  shell '/bin/bash'
  password '$1$rBLgRlwH$Bn2unBx6vv8LRSzz9Hiun1'
  system true
end

ruby_block "add hhuser to sudoers" do
  block do
    file = Chef::Util::FileEdit.new("/etc/sudoers")
    file.insert_line_if_no_match("hhuser    ALL=(ALL:ALL) ALL", "hhuser    ALL=(ALL:ALL) ALL")
    file.insert_line_if_no_match("hhuser     ALL = NOPASSWD: /bin/sync", "hhuser     ALL = NOPASSWD: /bin/sync")
    file.insert_line_if_no_match("hhuser     ALL = NOPASSWD: /sbin/sysctl vm.drop_caches=3", "hhuser     ALL = NOPASSWD: /sbin/sysctl vm.drop_caches=3")
    file.write_file
  end
end

