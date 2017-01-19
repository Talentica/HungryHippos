/**
 * 
 */
package com.talentica.hdfs.spark.binary.dataframe;

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
import com.talentica.hungryhippos.config.sharding.Column;

/**
 * @author pooshans
 *
 */
public class HdfsDataFrameMain {

  private static JavaSparkContext context;

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
    List<Column> columns =
        dataDescriptionConfig.getShardingClientConf().getInput().getDataDescription().getColumn();
    Broadcast<Integer> columnVal = context.broadcast(columns.size());
    SparkSession sparkSession =
        SparkSession.builder().master("local[*]").appName("test").getOrCreate();
    JavaRDD<AbstractTuple> rdd1 = rdd.mapPartitions(new FlatMapFunction<Iterator<byte[]>, AbstractTuple>() {
      @Override
      public Iterator<AbstractTuple> call(Iterator<byte[]> t) throws Exception {
        List<AbstractTuple> tupleList = new ArrayList<AbstractTuple>();
        while (t.hasNext()) {
          byte[] b = t.next();
          hhrddRowReader.setByteBuffer(ByteBuffer.wrap(b));
          tupleList.add(TupleBuilder.getRow(hhrddRowReader, columnVal.getValue()));
        }
        return tupleList.iterator();
      }
    }, true);
    Dataset<Row> rows = sparkSession.sqlContext().createDataFrame(rdd1, Tuple.class);
    rows.createOrReplaceTempView("data");
    Dataset<Row> rs = sparkSession.sql("SELECT * FROM data ");
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
