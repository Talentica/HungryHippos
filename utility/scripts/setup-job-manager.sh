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
jobUuid=$1
sh shut-down-job-manager.sh $jobUuid
sh cleanup-job-manager.sh $jobUuid
echo 'Copying new build'
sh copy-file-to-job-manager.sh ../lib/job-manager.jar $jobUuid
echo 'Copying test jobs jar'
sh copy-file-to-job-manager.sh ../lib/$jobUuid/test-jobs.jar $jobUuid