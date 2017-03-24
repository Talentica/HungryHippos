#!/lib/bash
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
echo '####       Deleting installation directory      ####'
rm -r ../../installation
echo '####       Creation installation directory      ####'
mkdir -p ../../installation ../../installation/conf ../../installation/lib
echo '####   Copying jars to installation directory   ####'
cp ../../client-api/build/libs/*.jar ../../installation/lib/
cp ../../common/build/libs/*.jar ../../installation/lib/
cp ../../configuration-schema/build/libs/*.jar ../../installation/lib/
cp ../../coordination/build/libs/*.jar ../../installation/lib/
cp ../../data-publisher/build/libs/*.jar ../../installation/lib/
cp ../../data-synchronizer/build/libs/*.jar ../../installation/lib/
cp ../../file-system/build/libs/*.jar ../../installation/lib/
cp ../../hungryhippos_tester-services-client/build/libs/*.jar ../../installation/lib/
cp ../../hungryhippos-tester-web/build/libs/*.jar ../../installation/lib/
cp ../../job-manager/build/libs/*.jar ../../installation/lib/
cp ../../node/build/libs/*.jar ../../installation/lib/
cp ../../sharding/build/libs/*.jar ../../installation/lib/
cp ../../spark/build/libs/*.jar ../../installation/lib/
cp ../../storage/build/libs/*.jar ../../installation/lib/
cp ../../test-hungry-hippos/build/libs/*.jar ../../installation/lib/
cp ../../utility/build/libs/*.jar ../../installation/lib/
cp ../../configuration-schema/src/main/resources/distribution/*.xml ../../installation/conf
echo '####            Completed                       ####'
