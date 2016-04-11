echo '################          START SHARDING      ################'
sh start-sharding.sh
echo '################          SHARDING INITIATED      ################'

echo '################          START DATA-PUBLISHING      ################'
sh start-data-publisher.sh
echo '################          DATA-PUBLISHING INITIATED      ################'

echo '################          START JOB MATRIX      ################'
sh start-job-manager.sh  com.talentica.hungryHippos.test.sum.SumJobMatrixImpl
echo '################          JOB MATRIX INITIATED      ################'