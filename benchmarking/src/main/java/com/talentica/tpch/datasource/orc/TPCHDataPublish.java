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

package com.talentica.tpch.datasource.orc;

import com.talentica.hungryhippos.datasource.HHSchemaReader;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Created by rajkishoreh.
 */
public class TPCHDataPublish {
    private static JavaSparkContext context;
    private static SparkSession sparkSession;

    public static void main(String[] args) {
        String appName = args[0];
        String inputDataFolderPath = args[1];
        String orcDataFolderPath = args[2];
        String shardInfoFoldersPath = args[3];
        initSparkContext(appName);


        System.out.println("Started publishing nation "+new Date());
        StructType nationSchema = HHSchemaReader.readSchema(shardInfoFoldersPath+"NATION");
        publishData(nationSchema,inputDataFolderPath,orcDataFolderPath,"nation.tbl");
        System.out.println("Completed publishing nation "+new Date());
        System.out.println("Started publishing region "+new Date());
        StructType regionSchema = HHSchemaReader.readSchema(shardInfoFoldersPath+"REGION");
        publishData(regionSchema,inputDataFolderPath,orcDataFolderPath,"region.tbl");
        System.out.println("Completed publishing region "+new Date());
        System.out.println("Started publishing customer "+new Date());
        StructType customerSchema = HHSchemaReader.readSchema(shardInfoFoldersPath+"CUSTOMER");
        publishData(customerSchema,inputDataFolderPath,orcDataFolderPath,"customer.tbl");
        System.out.println("Completed publishing customer "+new Date());
        System.out.println("Started publishing lineitem "+new Date());
        StructType lineItemSchema = HHSchemaReader.readSchema(shardInfoFoldersPath+"LINEITEM");
        publishData(lineItemSchema,inputDataFolderPath,orcDataFolderPath,"lineitem.tbl");
        System.out.println("Completed publishing lineitem "+new Date());
        System.out.println("Started publishing supplier "+new Date());
        StructType supplierSchema = HHSchemaReader.readSchema(shardInfoFoldersPath+"SUPPLIER");
        publishData(supplierSchema,inputDataFolderPath,orcDataFolderPath,"supplier.tbl");
        System.out.println("Completed publishing supplier "+new Date());
        System.out.println("Started publishing orders "+new Date());
        StructType ordersSchema = HHSchemaReader.readSchema(shardInfoFoldersPath+"ORDERS");
        publishData(ordersSchema,inputDataFolderPath,orcDataFolderPath,"orders.tbl");
        System.out.println("Completed publishing orders "+new Date());
        System.out.println("Started publishing part "+new Date());
        StructType partSchema = HHSchemaReader.readSchema(shardInfoFoldersPath+"PART");
        publishData(partSchema,inputDataFolderPath,orcDataFolderPath,"part.tbl");
        System.out.println("Completed publishing part "+new Date());
        System.out.println("Started publishing partsupp "+new Date());
        StructType partSuppSchema = HHSchemaReader.readSchema(shardInfoFoldersPath+"PARTSUPP");
        publishData(partSuppSchema,inputDataFolderPath,orcDataFolderPath,"partsupp.tbl");
        System.out.println("Completed publishing partsupp "+new Date());
        sparkSession.stop();
        context.stop();

    }


    public static void publishData(StructType tpchSchema, String filePath, String orcSchemaPath, String fileName){
        Dataset<Row> rowDataset = sparkSession.read().format("com.databricks.spark.csv").schema(tpchSchema)
                .option("header", "false").option("delimiter", "|").load(filePath+fileName);
        rowDataset.write().orc(orcSchemaPath + fileName);
    }

    private static void initSparkContext(String appName) {
        sparkSession =
                SparkSession.builder().appName(appName).getOrCreate();
        context = new JavaSparkContext(sparkSession.sparkContext());
    }
}
