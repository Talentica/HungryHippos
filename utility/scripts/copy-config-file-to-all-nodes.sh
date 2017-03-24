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
echo 'Copying configuration file to all nodes'
sh copy-file-to-all-nodes.sh ../../utility/src/main/resources/config.properties
echo 'Copying configuration file to sharding node'
sh copy-file-to-sharding.sh ../../utility/src/main/resources/config.properties
echo 'Copying configuration file to job manager node'
sh copy-file-to-job-manager.sh ../../utility/src/main/resources/config.properties
echo 'Copying configuration file to data publisher node'
sh copy-file-to-data-publisher.sh ../../utility/src/main/resources/config.properties
