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
if [[ ( -z "$1") || ( -z "$2" )  ]];
then
	echo "provide zookeeper ip (i.e:- 127.0.0.1:2181) and ip of node where you want to start it on cluster "
	exit
fi
java com.talentica.torrent.TorrentTrackerStarter $1 $2 6969 >../logs/data-trac.out 2>../logs/data-trac.err
