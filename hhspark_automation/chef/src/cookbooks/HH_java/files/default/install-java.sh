#!/bin/bash
# installing java 8
add-apt-repository -y ppa:webupd8team/java
# added repository webupd8team
apt-get -y update

#agree to the terms
echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | debconf-set-selections
echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 seen true" | debconf-set-selections

#installing java 8
apt-get  -y install oracle-java8-installer
#installed java8 successfully

#echo "checking java version"
javac -version

# "setting java 8 as default java"
apt-get install oracle-java8-set-default

echo "done"
