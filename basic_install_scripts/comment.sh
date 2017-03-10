#!/bin/bash

#comment line number 80 of helper.rb which contains hardcoded value control persist 10m
sed -i '80 s/^/#/' /opt/vagrant/embedded/gems/gems/vagrant-1.8.5/plugins/synced_folders/rsync/helper.rb
