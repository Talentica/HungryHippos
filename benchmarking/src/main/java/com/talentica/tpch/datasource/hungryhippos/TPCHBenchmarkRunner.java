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

package com.talentica.tpch.datasource.hungryhippos;

import com.talentica.hungryhippos.datasource.HHSparkContext;
import com.talentica.tpch.TPCHQuery;
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
public class TPCHBenchmarkRunner {
    private static HHSparkContext context;
    private static SparkSession sparkSession;
    protected static Logger LOGGER = LoggerFactory.getLogger(TPCHBenchmarkRunner.class);

    public static void main(String[] args) throws JAXBException, IOException, InterruptedException {

        String appName = args[0];
        String hhDataFolderPath = args[1];
        String clientConfigPath = args[2];
        initSparkContext(appName, clientConfigPath);

        SQLContext sqlContext = new SQLContext(context);
        Dataset<Row> df = sqlContext.read().
                format("com.talentica.hungryhippos.datasource").
                option("dimension","0").
                load(hhDataFolderPath + "CUSTOMER");
        df.createOrReplaceTempView("CUSTOMER");
        df = sqlContext.read().
                format("com.talentica.hungryhippos.datasource").
                option("dimension","0").
                load(hhDataFolderPath + "LINEITEM");
        df.createOrReplaceTempView("LINEITEM");
        df = sqlContext.read().
                format("com.talentica.hungryhippos.datasource").
                option("dimension","0").
                load(hhDataFolderPath + "NATION");
        df.createOrReplaceTempView("NATION");
        df = sqlContext.read().
                format("com.talentica.hungryhippos.datasource").
                option("dimension","0").
                load(hhDataFolderPath + "ORDERS");
        df.createOrReplaceTempView("ORDERS");
        df = sqlContext.read().
                format("com.talentica.hungryhippos.datasource").
                option("dimension","0").
                load(hhDataFolderPath + "PART");
        df.createOrReplaceTempView("PART");
        df = sqlContext.read().
                format("com.talentica.hungryhippos.datasource").
                option("dimension","0").
                load(hhDataFolderPath + "PARTSUPP");
        df.createOrReplaceTempView("PARTSUPP");
        df = sqlContext.read().
                format("com.talentica.hungryhippos.datasource").
                option("dimension","0").
                load(hhDataFolderPath + "REGION");
        df.createOrReplaceTempView("REGION");
        df = sqlContext.read().
                format("com.talentica.hungryhippos.datasource").
                option("dimension","0").
                load(hhDataFolderPath + "SUPPLIER");
        df.createOrReplaceTempView("SUPPLIER");

        int queryId = 1;
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
