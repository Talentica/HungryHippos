/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDInfo;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;

/**
 * @author pooshans
 *
 */
public class HHDataFrameMain {

  private static JavaSparkContext context;

  @SuppressWarnings("serial")
  public static void main(String[] args) throws ClassNotFoundException, InstantiationException,
      IllegalAccessException, NoSuchFieldException, SecurityException, IllegalArgumentException,
      NoSuchMethodException, InvocationTargetException, FileNotFoundException, JAXBException {
    String masterIp = args[0];
    String appName = args[1];
    String hhFilePath = args[2];
    String clientConfigPath = args[3];
    initSparkContext(masterIp, appName);
    HHRDDHelper.initialize(clientConfigPath);
    HHRDDInfo hhrddInfo = HHRDDHelper.getHhrddInfo(hhFilePath);
    HHRDD hipposRDD = new HHRDD(context, hhrddInfo, false);
    HHRDDRowReader hhrddRowReader = new HHRDDRowReader(hhrddInfo.getFieldDataDesc());
    Broadcast<HHRDDRowReader> rowReader = context.broadcast(hhrddRowReader);
    SparkSession sparkSession =
        SparkSession.builder().master(masterIp).appName(appName).getOrCreate();
    JavaRDD<HHTuple> rddForDataframe = hipposRDD.toJavaRDD().map(new Function<byte[], HHTuple>() {
      HHRDDRowReader hhrddRowReader = rowReader.getValue();

      @Override
      public HHTuple call(byte[] v1) throws Exception {
        hhrddRowReader.setByteBuffer(ByteBuffer.wrap(v1));
        return new HHTupleType<HHTuple>(hhrddRowReader) {
          @Override
          protected HHTuple createTuple() {
            return new HHTuple();
          }
        }.getTuple();
      }
    });
    Dataset<Row> rows = sparkSession.sqlContext().createDataFrame(rddForDataframe, HHTuple.class);
    rows.createOrReplaceTempView("TableView");
    Dataset<Row> rs = sparkSession
        .sql("SELECT * FROM TableView WHERE key1 LIKE 'a' and key2 LIKE 'a' and key3 LIKE 'a' ");
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
