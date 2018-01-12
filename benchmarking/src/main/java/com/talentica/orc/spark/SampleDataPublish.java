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

import com.talentica.hungryhippos.datasource.HHSchemaReader;
import com.talentica.tpch.TPCHSchema;
import com.talentica.tpch.TPCHSchemaFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class SampleDataPublish {
    private static JavaSparkContext context;
    private static SparkSession sparkSession;
    protected static Logger LOGGER = LoggerFactory.getLogger(SampleDataPublish.class);

    public static void main(String[] args) {

        String appName = args[0];
        String shardingFolderPath = args[1];
        String filePath = args[2];
        String orcSchemaPath = args[3];
        initSparkContext(appName);

        final StructType schema = HHSchemaReader.readSchema(shardingFolderPath);

        System.out.println("Started publishing " + new Date());
        publishData(schema, filePath, orcSchemaPath);
        System.out.println("Completed publishing " + new Date());
        sparkSession.stop();
        context.stop();

    }

    public static void publishData(StructType schema, String filePath, String orcSchemaPath) {
        Dataset<Row> rowDataset = sparkSession.read().format("com.databricks.spark.csv").schema(schema)
                .option("header", "false").option("delimiter", ",").load(filePath);
        rowDataset.write().orc(orcSchemaPath);
    }


    private static void initSparkContext(String appName) {
        sparkSession =
                SparkSession.builder().appName(appName).getOrCreate();
        context = new JavaSparkContext(sparkSession.sparkContext());

    }
}
