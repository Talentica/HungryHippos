/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryhippos.examples;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.rdd.HHSparkContext;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;
import com.talentica.hungryHippos.rdd.utility.HHRDDFileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Computes sum of 5th column values grouped by first column.
 * Creates partitions bucket wise, so results could be computed without shuffling.
 * <b>Note</b> : Do not use this strategy, when you want to perform a
 * group by operation using only non-sharded columns.
 *
 * Created by rajkishoreh on 16/3/17.
 */
public class SumJobSpecialCase {

    public static void main(String[] args) throws IOException, JAXBException {
        validateProgramArgument(args);
        String masterIp = args[0];
        String appName = args[1];
        String hhFilePath = args[2];
        String clientConfigPath = args[3];
        String outputDirectory = args[4];
        SparkConf conf = new SparkConf().setMaster(masterIp).setAppName(appName);
        HHSparkContext hhSparkContext = new HHSparkContext(conf, clientConfigPath);

        //array of column indexes on which group by operation has to be performed
        Integer[] jobDimensions = {0};

        //Providing requiresShuffle as false in the below method call creates spark partitions for RDD according to the buckets
        JavaRDD<byte[]> javaRDD = hhSparkContext.binaryRecords(jobDimensions, hhFilePath, false);

        Broadcast<DataDescription> dataDescriptionBroadcast = hhSparkContext.broadcastFieldDataDescription(hhFilePath);
        JavaPairRDD<String, Long> javaPairRDD = javaRDD.mapToPair(x -> {

            ByteBuffer byteBuffer = ByteBuffer.wrap(x);
            HHRDDRowReader hhrddRowReader = new HHRDDRowReader(dataDescriptionBroadcast.getValue(),byteBuffer);
            //using column 1 and column 5 for creating Pair RDD
            String key = hhrddRowReader.readAtColumn(0).toString();
            Integer value = (Integer) hhrddRowReader.readAtColumn(4);
            return new Tuple2<>(key, value.longValue());

        });


        //Method 1 : without shuffling
        JavaPairRDD<String, Long> sumPairRDD = javaPairRDD.mapPartitionsToPair(tuple2Iterator -> {
                    Map<String, Long> sumMap = new HashMap<>();
                    Long storedVal = 0l;
                    while (tuple2Iterator.hasNext()) {
                        Tuple2<String, Long> longTuple2 = tuple2Iterator.next();
                        storedVal = sumMap.get(longTuple2._1);
                        if(storedVal==null){
                            sumMap.put(longTuple2._1,longTuple2._2);
                        }else{
                            sumMap.put(longTuple2._1, storedVal + longTuple2._2);
                        }
                    }
                    return sumMap.entrySet().parallelStream()
                            .map(x -> new Tuple2<>(x.getKey(), x.getValue())).iterator();
                }
        );

        //Method 2 : with shuffling also possible
        //JavaPairRDD<String , Long> sumPairRDD = javaPairRDD.combineByKey(x->x,(x,y)->x+y,(x,y)->x+y)
        // .reduceByKey((x,y)->x+y);


        String actualPath = hhSparkContext.getActualPath(outputDirectory);
        HHRDDFileUtils.saveAsText(sumPairRDD, actualPath);
    }

    private static void validateProgramArgument(String args[]) {
        if (args.length < 5) {
            System.err.println(
                    "Improper arguments. Please provide in  proper order. i.e <spark-master-ip> <application-name> <distributed-directory> <client-configuration> <ouput-file-name>");
            System.out.println(
                    "Parameter argumes should be {spark://{master}:7077} {test-app} {/distr/data} {{client-path}/client-config.xml} {output}");
            System.exit(1);
        }
    }
}
