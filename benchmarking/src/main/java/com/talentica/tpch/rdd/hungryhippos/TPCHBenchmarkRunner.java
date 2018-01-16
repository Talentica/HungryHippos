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

package com.talentica.tpch.rdd.hungryhippos;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.rdd.HHSparkContext;
import com.talentica.hungryhippos.datasource.HHSchemaReader;
import com.talentica.tpch.*;
import com.talentica.tpch.rdd.TPCHRDDGenerator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rajkishoreh on 27/6/17.
 */
public class TPCHBenchmarkRunner {
    private static HHSparkContext context;
    private static SparkSession sparkSession;
    private static Map<String,StructType> schemaMap;

    public static void main(String[] args) throws JAXBException, IOException, InterruptedException {

        String appName = args[0];
        String inputDataFolderPath = args[1];
        String clientConfigPath = args[2];
        String shardInfoFoldersPath= args[3];
        initSparkContext(appName, clientConfigPath);

        schemaMap = new HashMap<>();
        registerStructType(shardInfoFoldersPath, "NATION");
        registerStructType(shardInfoFoldersPath, "REGION");
        registerStructType(shardInfoFoldersPath, "LINEITEM");
        registerStructType(shardInfoFoldersPath, "CUSTOMER");
        registerStructType(shardInfoFoldersPath, "SUPPLIER");
        registerStructType(shardInfoFoldersPath, "ORDERS");
        registerStructType(shardInfoFoldersPath, "PART");
        registerStructType(shardInfoFoldersPath, "PARTSUPP");


        Integer[] jobDimensions = {0};
        generateTable(jobDimensions, inputDataFolderPath, "NATION");
        generateTable(jobDimensions, inputDataFolderPath, "REGION");
        generateTable(jobDimensions, inputDataFolderPath, "LINEITEM");
        generateTable(jobDimensions, inputDataFolderPath, "CUSTOMER");
        generateTable(jobDimensions, inputDataFolderPath, "SUPPLIER");
        generateTable(jobDimensions, inputDataFolderPath, "ORDERS");
        generateTable(jobDimensions, inputDataFolderPath, "PART");
        generateTable(jobDimensions, inputDataFolderPath, "PARTSUPP");



        jobDimensions[0] = 8;
        generateTable(jobDimensions, inputDataFolderPath, "LINEITEM");
        Dataset<Row> query;
        query = sparkSession.sql(TPCHQuery.sql1);
        query.show();


        jobDimensions[0] = 4;
        generateTable(jobDimensions, inputDataFolderPath, "PART");
        jobDimensions[0] = 1;
        generateTable(jobDimensions, inputDataFolderPath, "PARTSUPP");
        jobDimensions[0] = 0;
        generateTable(jobDimensions, inputDataFolderPath , "REGION");
        jobDimensions[0] = 3;
        generateTable(jobDimensions, inputDataFolderPath , "SUPPLIER");
        jobDimensions[0] = 0;
        generateTable(jobDimensions, inputDataFolderPath , "NATION");
        query = sparkSession.sql(TPCHQuery.sql2);
        query.show();


        jobDimensions[0] = 6;
        generateTable(jobDimensions, inputDataFolderPath , "CUSTOMER");

        jobDimensions[0] = 5;
        generateTable(jobDimensions, inputDataFolderPath , "ORDERS");
        query = sparkSession.sql(TPCHQuery.sql3);
        query.show();


        jobDimensions[0] = 5;
        generateTable(jobDimensions, inputDataFolderPath , "ORDERS");
        query = sparkSession.sql(TPCHQuery.sql4);
        query.show();


        jobDimensions[0] = 3;
        generateTable(jobDimensions, inputDataFolderPath , "CUSTOMER");

        jobDimensions[0] = 3;
        generateTable(jobDimensions, inputDataFolderPath , "SUPPLIER");
        query = sparkSession.sql(TPCHQuery.sql5);
        query.show();


        query = sparkSession.sql(TPCHQuery.sql6);
        query.show();


        jobDimensions[0] = 3;
        generateTable(jobDimensions, inputDataFolderPath , "CUSTOMER");
        jobDimensions[0] = 3;
        generateTable(jobDimensions, inputDataFolderPath , "SUPPLIER");
        sparkSession.sql(TPCHQuery.subquerySql7).createOrReplaceTempView("SHIPPING");
        query = sparkSession.sql(TPCHQuery.sqlWithSubquerySql7);
        query.show();


        jobDimensions[0] = 4;
        generateTable(jobDimensions, inputDataFolderPath , "PART");
        jobDimensions[0] = 3;
        generateTable(jobDimensions, inputDataFolderPath , "SUPPLIER");
        jobDimensions[0] = 3;
        generateTable(jobDimensions, inputDataFolderPath , "CUSTOMER");

        query = sparkSession.sql(TPCHQuery.sql8);
        query.show();


        jobDimensions[0] = 3;
        generateTable(jobDimensions, inputDataFolderPath , "SUPPLIER");
        jobDimensions[0] = 1;
        generateTable(jobDimensions, inputDataFolderPath , "PARTSUPP");
        query = sparkSession.sql(TPCHQuery.sql9);
        query.show();


        jobDimensions[0] = 3;
        generateTable(jobDimensions, inputDataFolderPath , "CUSTOMER");
        jobDimensions[0] = 8;
        generateTable(jobDimensions, inputDataFolderPath , "LINEITEM");
        query = sparkSession.sql(TPCHQuery.sql10);
        query.show();


        jobDimensions[0] = 1;
        generateTable(jobDimensions, inputDataFolderPath , "PARTSUPP");
        jobDimensions[0] = 3;
        generateTable(jobDimensions, inputDataFolderPath , "SUPPLIER");
        query = sparkSession.sql(TPCHQuery.sql11);
        query.show();


        jobDimensions[0] = 14;
        generateTable(jobDimensions, inputDataFolderPath , "LINEITEM");
        jobDimensions[0] = 5;
        generateTable(jobDimensions, inputDataFolderPath , "ORDERS");
        query = sparkSession.sql(TPCHQuery.sql12);
        query.show();


        query = sparkSession.sql(TPCHQuery.sql13);
        query.show();


        jobDimensions[0] = 4;
        generateTable(jobDimensions, inputDataFolderPath , "PART");
        query = sparkSession.sql(TPCHQuery.sql14);
        query.show();


        sparkSession.sql(TPCHQuery.sqlView15).createOrReplaceTempView("REVENUE0");
        query = sparkSession.sql(TPCHQuery.sqlWithView15);
        query.show();


        jobDimensions[0] = 5;
        generateTable(jobDimensions, inputDataFolderPath , "PART");
        jobDimensions[0] = 1;
        generateTable(jobDimensions, inputDataFolderPath , "PARTSUPP");
        query = sparkSession.sql(TPCHQuery.sql16);
        query.show();


        jobDimensions[0] = 3;
        generateTable(jobDimensions, inputDataFolderPath , "PART");
        query = sparkSession.sql(TPCHQuery.sql17);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql18);
        query.show();


        jobDimensions[0] = 3;
        generateTable(jobDimensions, inputDataFolderPath , "PART");
        jobDimensions[0] = 14;
        generateTable(jobDimensions, inputDataFolderPath , "LINEITEM");
        query = sparkSession.sql(TPCHQuery.sql19);
        query.show();


        jobDimensions[0] = 3;
        generateTable(jobDimensions, inputDataFolderPath , "SUPPLIER");
        jobDimensions[0] = 1;
        generateTable(jobDimensions, inputDataFolderPath , "PARTSUPP");
        query = sparkSession.sql(TPCHQuery.sql20);
        query.show();


        jobDimensions[0] = 2;
        generateTable(jobDimensions, inputDataFolderPath , "ORDERS");
        jobDimensions[0] = 3;
        generateTable(jobDimensions, inputDataFolderPath ,"SUPPLIER");

        query = sparkSession.sql(TPCHQuery.sql21);
        query.show();


        jobDimensions[0] = 3;
        generateTable(jobDimensions, inputDataFolderPath , "CUSTOMER");
        query = sparkSession.sql(TPCHQuery.sql22);
        query.show();
        sparkSession.stop();
        context.stop();
    }

    private static void registerStructType(String shardFoldersPath, String tableName) {
        StructType schema = HHSchemaReader.readSchema(shardFoldersPath+tableName);
        schemaMap.put(tableName,schema);
    }

    private static void generateTable(Integer[] jobDimensions, String schemaPath, String tableName) throws JAXBException, IOException {
        DataDescription dataDescription = context.getFieldDataDescription(schemaPath+tableName);
        JavaRDD<byte[]> javaRDD = context.binaryRecords(jobDimensions, schemaPath+tableName, true);
        TPCHRDDGenerator.createOrReplaceTempView(sparkSession, javaRDD, dataDescription, context, schemaMap.get(tableName),tableName);
    }

    protected static void initSparkContext(String appName,
                                           String clientConfigPath) {
        try {
            sparkSession =
                    SparkSession.builder().appName(appName).getOrCreate();
            context = new HHSparkContext(sparkSession.sparkContext(), clientConfigPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
