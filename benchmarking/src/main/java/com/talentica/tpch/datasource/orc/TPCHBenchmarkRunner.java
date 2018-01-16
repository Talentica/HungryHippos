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

import com.talentica.tpch.TPCHQuery;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by rajkishoreh.
 */
public class TPCHBenchmarkRunner {
    private static JavaSparkContext context;
    private static SparkSession sparkSession;

    public static void main(String[] args) {
        String appName = args[0];
        String orcDataFolderPath = args[1];
        initSparkContext(appName);

        generateTable(orcDataFolderPath+"customer.tbl","CUSTOMER");
        generateTable(orcDataFolderPath+"lineitem.tbl","LINEITEM");
        generateTable(orcDataFolderPath+"nation.tbl","NATION");
        generateTable(orcDataFolderPath+"orders.tbl","ORDERS");
        generateTable(orcDataFolderPath+"part.tbl","PART");
        generateTable(orcDataFolderPath+"partsupp.tbl","PARTSUPP");
        generateTable(orcDataFolderPath+"region.tbl", "REGION");
        generateTable(orcDataFolderPath+"supplier.tbl","SUPPLIER");
        int queryId =1;
        Dataset<Row> query;
        query = sparkSession.sql(TPCHQuery.sql1);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        query = sparkSession.sql(TPCHQuery.sql2);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        query = sparkSession.sql(TPCHQuery.sql3);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        query = sparkSession.sql(TPCHQuery.sql4);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        query = sparkSession.sql(TPCHQuery.sql5);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        query = sparkSession.sql(TPCHQuery.sql6);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        sparkSession.sql(TPCHQuery.subquerySql7).createOrReplaceTempView("SHIPPING");
        query = sparkSession.sql(TPCHQuery.sqlWithSubquerySql7);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        query = sparkSession.sql(TPCHQuery.sql8);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        query = sparkSession.sql(TPCHQuery.sql9);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        query = sparkSession.sql(TPCHQuery.sql10);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        query = sparkSession.sql(TPCHQuery.sql11);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        query = sparkSession.sql(TPCHQuery.sql12);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        query = sparkSession.sql(TPCHQuery.sql13);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        query = sparkSession.sql(TPCHQuery.sql14);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        sparkSession.sql(TPCHQuery.sqlView15).createOrReplaceTempView("REVENUE0");
        query = sparkSession.sql(TPCHQuery.sqlWithView15);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        query = sparkSession.sql(TPCHQuery.sql16);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        query = sparkSession.sql(TPCHQuery.sql17);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        query = sparkSession.sql(TPCHQuery.sql18);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        query = sparkSession.sql(TPCHQuery.sql19);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        query = sparkSession.sql(TPCHQuery.sql20);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        query = sparkSession.sql(TPCHQuery.sql21);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        query = sparkSession.sql(TPCHQuery.sql22);
        System.out.println("Query Id : "+queryId);
        query.show();
        queryId++;
        sparkSession.stop();
        context.stop();
    }

    private static void generateTable(String filePath, String tableName) {
        Dataset<Row> rowDataset = sparkSession.read().orc(filePath);
        rowDataset.createOrReplaceTempView(tableName);
    }



    private static void initSparkContext(String appName) {
        sparkSession =
                SparkSession.builder().appName(appName).getOrCreate();
        context = new JavaSparkContext(sparkSession.sparkContext());

    }

}
