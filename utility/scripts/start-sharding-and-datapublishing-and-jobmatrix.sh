echo '################          START SHARDING      ################'
sh start-sharding.sh
echo '################          SHARDING INITIATED      ################'

echo '################          START DATA-PUBLISHING      ################'
sh start-data-publisher.sh
echo '################          DATA-PUBLISHING INITIATED      ################'

echo '################          START JOB MATRIX      ################'
sh start-job-manager.sh  $1 $2
echo '################          JOB MATRIX INITIATED      ################'