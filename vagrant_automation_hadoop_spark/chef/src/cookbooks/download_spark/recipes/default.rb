#
# Cookbook Name:: download_spark
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

cookbook_file "/usr/local/spark-2.0.2-bin-hadoop2.7.tgz" do
source "spark-2.0.2-bin-hadoop2.7.tgz"
mode "777"
end

execute "extract spark source" do
  command "tar -xzvf /usr/local/spark-2.0.2-bin-hadoop2.7.tgz"
end

execute "copy extracted file from / to /usr/local/spark-2.0.2-bin-hadoop2.7" do
  command "mv  /spark-2.0.2-bin-hadoop2.7 /usr/local/spark-2.0.2-bin-hadoop2.7"
end

execute "change ownership of hungryhippo folder to hduser" do
  command "chown hduser:hadoop -R /usr/local/spark-2.0.2-bin-hadoop2.7"
end

#execute "installing git" do 
#  command "apt-get install git -y"
#end

#execute "upating the repository" do
#  command "apt-get update -y"
#end

#execute "echo sbt" do
#  command "echo 'deb https://dl.bintray.com/sbt/debian /' | sudo tee -a /etc/apt/sources.list.d/sbt.list"
#end

#execute "adding keyserver" do
#  command "apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823"
#end

#execute "update repo for sbt" do
#  command "apt-get update"
#end

#execute "installing sbt" do 
#  command "apt-get install sbt -y"
#end

#execute "installing openssh server and openssh client" do
#  command "apt-get install openssh-server openssh-client -y"
#end

#execute "install scala" do 
 # command "apt-get install scala -y"
#end

#execute "update repository" do
#  command "apt-get update"
#end
	
#execute "install autoconf and libtool" do 
#  command "apt-get install -y autoconf libtool"
#end

#execute "install build-essential python-dev python-boto libcurl etc" do
#  command "apt-get -y install build-essential python-dev python-boto libcurl4-nss-dev libsasl2-dev maven libapr1-dev libsvn-dev"
#end

#execute "install keyserver" do
#  command "apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF"
#end

#execute "purge java 7" do
#  command "apt-get purge openjdk-\*"
#end
