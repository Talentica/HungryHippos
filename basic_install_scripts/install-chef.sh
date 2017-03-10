#!/bin/bash
apt-get -y remove chef-solo
curl -L https://www.opscode.com/chef/install.sh | bash
