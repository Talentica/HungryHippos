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

package com.talentica.tpch.rdd;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.nio.ByteBuffer;

/**
 * Created by rajkishoreh on 27/6/17.
 */
public class TPCHRDDGenerator {

    public static void createOrReplaceTempView(SparkSession sparkSession, JavaRDD<byte[]> javaRDD, DataDescription dataDescription, JavaSparkContext context, StructType tpchSchema, String tableName) {
        Broadcast<DataDescription> descriptionBroadcast = context.broadcast(dataDescription);
        JavaRDD<Row> rowRDD = javaRDD.map(x -> {
            ByteBuffer byteBuffer = ByteBuffer.wrap(x);
            HHRDDRowReader readerVar = new HHRDDRowReader(descriptionBroadcast.getValue(), byteBuffer);
            Object[] attributes = new Object[descriptionBroadcast.value().getNumberOfDataFields()];
            for (int i = 0; i < attributes.length; i++) {
                attributes[i] = readerVar.readAtColumn(i);
            }
            return RowFactory.create(attributes);
        });
        Dataset<Row> dataFrame = sparkSession.createDataFrame(rowRDD, tpchSchema);
        dataFrame.createOrReplaceTempView(tableName);
    }

}
