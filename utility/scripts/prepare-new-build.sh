#!/bin/bash

echo "Cleaning and building data-publisher jar"
cd ../../data-publisher/
gradle clean jar

echo "Cleaning and building digital-ocean-manager jar"
cd ../digital-ocean-manager/
gradle clean jar

echo "Cleaning and building test jobs jar"
cd ../test-hungry-hippos/
gradle clean jar

echo "Cleaning and building job-manager jar"
cd ../job-manager/
gradle clean jar

echo "Cleaning and building node jar"
cd ../node/
gradle clean jar

