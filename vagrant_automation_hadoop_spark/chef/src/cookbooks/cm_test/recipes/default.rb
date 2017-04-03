#
# Cookbook Name:: cm_test
# Recipe:: default
#
# Copyright 2016, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#
cookbook_file "/usr/share/test_cb_chint.txt" do
source "test_cb_chint_file.txt"
mode "0644"
end
