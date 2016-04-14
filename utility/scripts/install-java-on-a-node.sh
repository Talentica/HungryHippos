#!/bin/bash

echo "Installing Java on $1"
ssh -o StrictHostKeyChecking=no root@$1 "sudo dpkg --configure -a;sudo add-apt-repository ppa:webupd8team/java -y;sudo apt-get update;nohup sudo apt-get install oracle-java8-installer"
