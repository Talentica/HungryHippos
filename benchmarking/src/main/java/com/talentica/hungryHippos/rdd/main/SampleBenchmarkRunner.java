/*
 * *****************************************************************************
 *   Copyright 2017 Talentica Software Pvt. Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *  *****************************************************************************
 */

package com.talentica.hungryHippos.rdd.main;

import com.talentica.hungryHippos.rdd.HHSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.IOException;

/**
 * Created by rajkishoreh on 27/6/17.
 */
public class SampleBenchmarkRunner {
    private static HHSparkContext context;
    private static SparkSession sparkSession;
    protected static Logger LOGGER = LoggerFactory.getLogger(SampleBenchmarkRunner.class);

    public static void main(String[] args) throws JAXBException, IOException, InterruptedException {

        String appName = args[0];
        String schemaPath = args[1];
        String clientConfigPath = args[2];
        initSparkContext(appName, clientConfigPath);

        SQLContext sqlContext = new SQLContext(context);
        Dataset<Row> df = sqlContext.read().
                format("com.talentica.hungryhippos.datasource").
                option("dimension","0").
                load(schemaPath);
        df.createOrReplaceTempView("SAMPLEDATA");
        int queryId = 1;
        Dataset<Row> query;
        query = sparkSession.sql("SELECT key1, key2, key3 ,sum(key6) as sum1 , sum(key5) as sum2 FROM SAMPLEDATA GROUP BY key1,key2,key3 order by sum1 desc, sum2 desc");
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        sparkSession.stop();
        context.stop();
    }

    protected static void initSparkContext(String appName,
                                           String clientConfigPath) {
        if (context == null) {

            try {
                sparkSession =
                        SparkSession.builder().appName(appName).getOrCreate();
                context = new HHSparkContext(sparkSession.sparkContext(),clientConfigPath);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}
