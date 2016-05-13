jobMatrix=$1
jobUuid=$2

echo '################          START SHARDING      ################'
sh start-sharding.sh $jobUuid
echo '################          SHARDING INITIATED      ################'

echo '################          START DATA-PUBLISHING      ################'
sh start-data-publisher.sh $jobUuid
echo '################          DATA-PUBLISHING INITIATED      ################'

echo '################          START JOB MATRIX      ################'
sh start-job-manager.sh  $jobMatrix $jobUuid
echo '################          JOB MATRIX INITIATED      ################'