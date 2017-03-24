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
/**
 * 
 */
package com.talentica.hungryHippos.rdd.utility;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.util.Iterator;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * Utility tool to write RDD data into files partition wise
 * @author pooshans
 *
 */
public class HHRDDFileUtils implements Serializable {

  private static final long serialVersionUID = 4888239993511591404L;

    /**
     * Writes JavaRDD into files partition wise with a custom delimiter
     * @param javaRDD
     *          An instance of JavaRDD
     * @param path
     *          Absolute path
     */
    public static <T> void saveAsText(JavaRDD<T> javaRDD, String path) {
        javaRDD.foreachPartition(new VoidFunction<Iterator<T>>() {

            @Override
            public void call(Iterator<T> t) throws Exception {
                int partitionId = TaskContext.getPartitionId();
                new File(path).mkdirs();
                String filePath = path + File.separatorChar + "part-" + partitionId;
                File file = new File(filePath);
                BufferedWriter out = new BufferedWriter(new FileWriter(file), 20480);
                while (t.hasNext()) {
                    out.write(t.next().toString());
                    out.newLine();
                }
                out.flush();
                out.close();
            }
        });
    }

    /**
     * Writes JavaRDD of Tuple2 data into files partition wise with a custom delimiter
     * @param javaRDD
     *          An instance of JavaRDD
     * @param path
     *          Absolute path
     * @param delimiter
     *          A delimiter such as comma, tab etc.
     */
    public static <K,V> void saveAsText(JavaRDD<Tuple2<K,V>> javaRDD, String path, String delimiter) {
        javaRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<K,V>>>() {

            @Override
            public void call(Iterator<Tuple2<K,V>> t) throws Exception {
                int partitionId = TaskContext.getPartitionId();
                new File(path).mkdirs();
                String filePath = path + File.separatorChar + "part-" + partitionId;
                File file = new File(filePath);
                BufferedWriter out = new BufferedWriter(new FileWriter(file), 20480);
                Tuple2 tuple2 = null;
                while (t.hasNext()) {
                    tuple2 = t.next();
                    out.write(tuple2._1+delimiter+tuple2._2);
                    out.newLine();
                }
                out.flush();
                out.close();
            }
        });
    }

    /**
     * Writes JavaPairRDD data into files partition wise
     * @param javaPairRDD
     *          An instance of JavaPairRDD
     * @param path
     *          Absolute path
     */
	public static <K,V> void saveAsText(JavaPairRDD<K,V> javaPairRDD, String path) {

		javaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<K, V>>>() {
			@Override
			public void call(Iterator<Tuple2<K, V>> tuple2Iterator) throws Exception {
				int partitionId = TaskContext.getPartitionId();
				new File(path).mkdirs();
				String filePath = path + File.separatorChar + "part-" + partitionId;
				File file = new File(filePath);
				BufferedWriter out = new BufferedWriter(new FileWriter(file),20480);
                Tuple2 tuple2 = null;
				while (tuple2Iterator.hasNext()) {
					tuple2 = tuple2Iterator.next();
					out.write(tuple2.toString());
					out.newLine();
				}
				out.flush();
				out.close();
			}
		});
	}

    /**
     * Writes JavaPairRDD data into files partition wise with a custom delimiter
     * @param javaPairRDD
     *          An instance of JavaPairRDD
     * @param path
     *          Absolute path
     * @param delimiter
     *          A delimiter such as comma, tab etc.
     */
    public static <K,V> void saveAsText(JavaPairRDD<K,V> javaPairRDD, String path, String delimiter) {

        javaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<K, V>>>() {
            @Override
            public void call(Iterator<Tuple2<K, V>> tuple2Iterator) throws Exception {
                int partitionId = TaskContext.getPartitionId();
                new File(path).mkdirs();
                String filePath = path + File.separatorChar + "part-" + partitionId;
                File file = new File(filePath);
                BufferedWriter out = new BufferedWriter(new FileWriter(file),20480);
                Tuple2 tuple2 = null;
                while (tuple2Iterator.hasNext()) {
                    tuple2 = tuple2Iterator.next();
                    out.write(tuple2._1 + delimiter + tuple2._2);
                    out.newLine();
                }
                out.flush();
                out.close();
            }
        });
    }

}
