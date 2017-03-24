#!/bin/bash
#*******************************************************************************
# Copyright 2017 Talentica Software Pvt. Ltd.
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
cd /root/hungryhippos/
echo "Creating tmp directory"
rm -r tmp
mkdir tmp
echo "temp directory created"
echo "Copying files"
cp /root/hungryhippos/checkout/HungryHippos/utility/scripts/*zk-server* /root/hungryhippos/tmp/
cp /root/hungryhippos/checkout/HungryHippos/digital-ocean-manager/build/libs/digital-ocean.jar /root/hungryhippos/digital-ocean-manager/
cp /root/hungryhippos/checkout/HungryHippos/digital-ocean-manager/src/main/resources/json/* /root/hungryhippos/tmp/
cp /root/hungryhippos/checkout/HungryHippos/coordination/src/main/resources/config.properties /root/hungryhippos/tmp/
chmod -R 777 /root/hungryhippos/tmp/
chmod -R 777 /root/hungryhippos/digital-ocean-manager/
echo "Files are copied."
