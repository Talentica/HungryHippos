#
# Cookbook Name:: create_group_user
# Recipe:: default
#
# Copyright 2016, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#
group 'hadoop' do
  action :create
  group_name 'hadoop'
  gid '902'
  append true
end
user 'hduser' do
  supports :manage_home => true
  uid '1235'
  gid 902
  home '/home/hduser'
  shell '/bin/bash'
  password 'hduser'
end

ruby_block "add hduser to sudoers" do
  block do
    file = Chef::Util::FileEdit.new("/etc/sudoers")
    file.insert_line_if_no_match("hduser    ALL=(ALL:ALL) ALL", "hduser    ALL=(ALL:ALL) ALL")
    file.write_file
  end
end
