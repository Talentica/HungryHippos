/**
 * 
 */
package com.talentica.hungryHippos.spark.dataframe;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.talentica.hdfs.spark.binary.job.DataDescriptionConfig;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

/**
 * @author pooshans
 *
 */
public class HHDataFrameMain {

  private static JavaSparkContext context;

  @SuppressWarnings("serial")
  public static void main(String[] args) throws ClassNotFoundException, InstantiationException,
      IllegalAccessException, NoSuchFieldException, SecurityException, IllegalArgumentException,
      NoSuchMethodException, InvocationTargetException {
    String masterIp = args[0];
    String appName = args[1];
    String inputFile = args[2];
    String outputDir = args[3];
    String shardingFolderPath = args[4];
    initSparkContext(masterIp, appName);
    DataDescriptionConfig dataDescriptionConfig = new DataDescriptionConfig(shardingFolderPath);
    JavaRDD<byte[]> rdd = context.binaryRecords(inputFile, dataDescriptionConfig.getRowSize());
    HHRDDRowReader hhrddRowReader = new HHRDDRowReader(dataDescriptionConfig.getDataDescription());
    Broadcast<HHRDDRowReader> rowReader = context.broadcast(hhrddRowReader);
    SparkSession sparkSession =
        SparkSession.builder().master("local[*]").appName("test").getOrCreate();
    JavaRDD<HHTupleType<HHTuple>> rdd1 =
        rdd.mapPartitions(new FlatMapFunction<Iterator<byte[]>, HHTupleType<HHTuple>>() {
          @Override
          public Iterator<HHTupleType<HHTuple>> call(Iterator<byte[]> t) throws Exception {
            List<HHTupleType<HHTuple>> tupleList = new ArrayList<HHTupleType<HHTuple>>();
            HHRDDRowReader hhrddRowReader = rowReader.getValue();
            while (t.hasNext()) {
              byte[] b = t.next();
              hhrddRowReader.setByteBuffer(ByteBuffer.wrap(b));
              tupleList.add(HHTupleBuilder.getRow(rowReader.getValue()));
            }
            return tupleList.iterator();
          }
        }, true);
    Dataset<Row> rows = sparkSession.sqlContext().createDataFrame(rdd1, HHTuple.class);
    rows.createOrReplaceTempView("data");
    Dataset<Row> rs = sparkSession.sql("SELECT * FROM data WHERE key1 like 'a' ");
    rs.show();
    context.stop();
  }

  private static void initSparkContext(String masterIp, String appName) {
    if (context == null) {
      SparkConf conf = new SparkConf().setMaster(masterIp).setAppName(appName);
      context = new JavaSparkContext(conf);
    }
  }
}
