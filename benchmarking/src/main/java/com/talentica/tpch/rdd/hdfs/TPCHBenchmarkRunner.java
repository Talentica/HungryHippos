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

package com.talentica.tpch.rdd.hdfs;

import com.talentica.hdfs.spark.binary.job.DataDescriptionConfig;
import com.talentica.hungryhippos.datasource.HHSchemaReader;
import com.talentica.tpch.TPCHQuery;
import com.talentica.tpch.rdd.TPCHRDDGenerator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rajkishoreh on 27/6/17.
 */
public class TPCHBenchmarkRunner {
    private static JavaSparkContext context;
    private static SparkSession sparkSession;
    private static Map<String,StructType> schemaMap;

    public static void main(String[] args) throws JAXBException, FileNotFoundException, InterruptedException {
        String appName = args[0];
        String inputDataFolderPath = args[1];
        String shardInfoFoldersPath = args[2];
        initSparkContext(appName);
        schemaMap = new HashMap<>();
        registerStructType(shardInfoFoldersPath, "NATION");
        registerStructType(shardInfoFoldersPath, "REGION");
        registerStructType(shardInfoFoldersPath, "LINEITEM");
        registerStructType(shardInfoFoldersPath, "CUSTOMER");
        registerStructType(shardInfoFoldersPath, "SUPPLIER");
        registerStructType(shardInfoFoldersPath, "ORDERS");
        registerStructType(shardInfoFoldersPath, "PART");
        registerStructType(shardInfoFoldersPath, "PARTSUPP");

        generateTable(inputDataFolderPath + "nation.tbl", shardInfoFoldersPath,"NATION");
        generateTable(inputDataFolderPath + "region.tbl", shardInfoFoldersPath,"REGION");
        generateTable(inputDataFolderPath + "lineitem.tbl", shardInfoFoldersPath,"LINEITEM");
        generateTable(inputDataFolderPath + "customer.tbl", shardInfoFoldersPath,"CUSTOMER");
        generateTable(inputDataFolderPath + "supplier.tbl", shardInfoFoldersPath,"SUPPLIER");
        generateTable(inputDataFolderPath + "orders.tbl", shardInfoFoldersPath,"ORDERS");
        generateTable(inputDataFolderPath + "part.tbl", shardInfoFoldersPath,"PART");
        generateTable(inputDataFolderPath + "partsupp.tbl", shardInfoFoldersPath,"PARTSUPP");

        Dataset<Row> query;
        query = sparkSession.sql(TPCHQuery.sql1);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql2);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql3);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql4);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql5);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql6);
        query.show();

        sparkSession.sql(TPCHQuery.subquerySql7).createOrReplaceTempView("SHIPPING");
        query = sparkSession.sql(TPCHQuery.sqlWithSubquerySql7);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql8);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql9);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql10);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql11);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql12);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql13);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql14);
        query.show();

        sparkSession.sql(TPCHQuery.sqlView15).createOrReplaceTempView("REVENUE0");
        query = sparkSession.sql(TPCHQuery.sqlWithView15);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql16);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql17);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql18);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql19);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql20);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql21);
        query.show();

        query = sparkSession.sql(TPCHQuery.sql22);
        query.show();

        sparkSession.stop();
        context.stop();
    }

    private static void registerStructType(String shardFoldersPath, String tableName) {
        StructType schema = HHSchemaReader.readSchema(shardFoldersPath+tableName);
        schemaMap.put(tableName,schema);
    }

    private static void generateTable(String filePath, String shardingFolderPath, String tableName) throws JAXBException, FileNotFoundException {
        DataDescriptionConfig dataDescriptionConfig = new DataDescriptionConfig(shardingFolderPath+tableName);
        JavaRDD<byte[]> javaRDD = context.binaryRecords(filePath, dataDescriptionConfig.getRowSize());
        TPCHRDDGenerator.createOrReplaceTempView(sparkSession, javaRDD, dataDescriptionConfig.getDataDescription(), context, schemaMap.get(tableName),tableName);
    }


    private static void initSparkContext(String appName) {
        sparkSession =
                SparkSession.builder().appName(appName).getOrCreate();
        context = new JavaSparkContext(sparkSession.sparkContext());

    }


}
