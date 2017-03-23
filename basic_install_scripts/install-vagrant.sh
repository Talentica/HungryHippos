#!/bin/bash
#*******************************************************************************
# Copyright [2017] [Talentica Software Pvt. Ltd.]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#*******************************************************************************

#remove vagrant if already installed
rm -rf /opt/vagrant
rm -f /usr/bin/vagrant
rm -f ~/.vagrant.d

#download vagrant
wget https://releases.hashicorp.com/vagrant/1.8.5/vagrant_1.8.5_x86_64.deb

#configure vagrant
dpkg -i vagrant_1.8.5_x86_64.deb

#install linux-headers
apt-get -y install linux-headers-$(uname -r)

#reconfigure virtual box
dpkg-reconfigure virtualbox-dkms

#install vagrant plugin
vagrant plugin install vagrant-digitalocean

# vagrant box -> download box
vagrant box add digital_ocean https://github.com/smdahlen/vagrant-digitalocean/raw/master/box/digital_ocean.box

#install vagrant-triggers
vagrant plugin install vagrant-triggers
