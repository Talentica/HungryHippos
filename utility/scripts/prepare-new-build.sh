#!/bin/bash

echo "Cleaning and building sharding jar"
cd ../../sharding/
gradle clean test jar

echo "Cleaning and building data-publisher jar"
cd ../data-publisher/
gradle clean test jar

echo "Cleaning and building node jar"
cd ../node/
gradle clean test jar

echo "Cleaning and building test jobs jar"
cd ../test-hungry-hippos/
gradle clean test jar

echo "Cleaning and building job-manager jar"
cd ../job-manager/
gradle clean test jar
