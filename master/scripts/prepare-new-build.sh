#!/bin/bash
echo "Cleaning and building master jar"
cd ../../master/
gradle clean jar
echo "Cleaning and building node jar"
cd ../node/
gradle clean jar
echo "Cleaning and building test jobs jar"
cd ../test-hungry-hippos/
gradle clean jar
