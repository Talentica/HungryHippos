jobMatrix=$1
jobUuid=$2
dataparserclass=$3

echo '################          START SHARDING      ################'
sh start-sharding.sh $jobUuid $dataparserclass
echo '################          SHARDING INITIATED      ################'

echo '################          START DATA-PUBLISHING      ################'
sh start-data-publisher.sh $jobUuid $dataparserclass
echo '################          DATA-PUBLISHING INITIATED      ################'

echo '################          START JOB MATRIX      ################'
sh start-job-manager.sh  $jobMatrix $jobUuid
echo '################          JOB MATRIX INITIATED      ################'