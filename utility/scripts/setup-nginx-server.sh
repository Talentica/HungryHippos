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
 echo "Setting up ngnix server on  $1"
 ssh -o StrictHostKeyChecking=no root@$1 'sudo mkdir -p /usr/local/nginx/html;echo "Start Updating apt get";sudo apt-get update;echo "Finished Updating apt get";echo "Start installing nginx server";sudo apt-get install nginx;echo "Finished installing nginx server";echo "Starting nginx server";sudo service nginx start;echo "Nginx 	server started"';

sudo apt-get update
sudo apt-get install nginx
sudo service nginx start

