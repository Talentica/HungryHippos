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

package com.talentica.orc.spark;

import com.talentica.tpch.TPCHQuery;
import com.talentica.tpch.TPCHSchema;
import com.talentica.tpch.TPCHSchemaFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleBenchmarkRunner {
    private static JavaSparkContext context;
    private static SparkSession sparkSession;
    protected static Logger LOGGER = LoggerFactory.getLogger(com.talentica.tpch.hdfs.TPCHBenchmarkRunner.class);

    public static void main(String[] args) {
        String appName = args[0];
        String orcSchemaPath = args[1];
        initSparkContext(appName);


        generateTable(orcSchemaPath, "SAMPLEDATA");
        int queryId = 1;
        Dataset<Row> query;
        query = sparkSession.sql("SELECT key1, key2, key3 ,sum(key6) as sum1 , sum(key5) as sum2 FROM SAMPLEDATA GROUP BY key1,key2,key3 order by sum1 desc, sum2 desc");
        System.out.println("Query Id : " + queryId);
        query.show();
        queryId++;
        sparkSession.stop();
        context.stop();
    }

    private static void generateTable(String filePath, String schemaName) {
        Dataset<Row> rowDataset = sparkSession.read().orc(filePath);
        rowDataset.createOrReplaceTempView(schemaName);
    }


    private static void initSparkContext(String appName) {
        sparkSession =
                SparkSession.builder().appName(appName).getOrCreate();
        context = new JavaSparkContext(sparkSession.sparkContext());
    }

}
