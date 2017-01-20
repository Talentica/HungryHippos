/**
 * 
 */
package com.talentica.hungryHippos.spark.dataframe;

import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
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
    JavaRDD<HHTupleType<HHTuple>> rddForDataframe = hipposRDD.toJavaRDD()
        .mapPartitions(new FlatMapFunction<Iterator<byte[]>, HHTupleType<HHTuple>>() {
          @Override
          public Iterator<HHTupleType<HHTuple>> call(Iterator<byte[]> t) throws Exception {
            List<HHTupleType<HHTuple>> tupleList = new ArrayList<HHTupleType<HHTuple>>();
            HHRDDRowReader hhrddRowReader = rowReader.getValue();
            while (t.hasNext()) {
              byte[] b = t.next();
              hhrddRowReader.setByteBuffer(ByteBuffer.wrap(b));
              tupleList.add(HHTupleBuilder.getHHTuple(rowReader.getValue()));
            }
            return tupleList.iterator();
          }
        }, true);
    Dataset<Row> rows = sparkSession.sqlContext().createDataFrame(rddForDataframe, HHTuple.class);
    rows.createOrReplaceTempView("TableView");
    Dataset<Row> rs = sparkSession
        .sql("SELECT * FROM TableView WHERE key1 like 'a' and key2 like 'a' and key3 like 'a' ");
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
