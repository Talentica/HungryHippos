/**
 * 
 */
package com.talentica.spark.test;

import java.io.FileNotFoundException;

import javax.activation.UnsupportedDataTypeException;
import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import com.talentica.hungryHippos.dataframe.HHDatasetBuilder;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDInfo;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;

/**
 * @author pooshans
 *
 */
public class HHDatasetTestMain {

  private static String masterIp;
  private static String appName;
  private static String hhFilePath;
  private static String clientConfigPath;
  private static JavaSparkContext context;
  private static HHRDDInfo hhrddInfo;
  private static HHRDD hhWithoutJobRDD;
  private static SparkSession sparkSession;
  private static HHDatasetBuilder hhDSWithoutJobBuilder;

  public static void main(String[] args)
      throws UnsupportedDataTypeException, FileNotFoundException, JAXBException {
    masterIp = "local[*]";
    appName = "testApp";
    hhFilePath = "/distr/data";
    clientConfigPath =
        "/home/pooshans/HungryHippos/HungryHippos/configuration-schema/src/main/resources/distribution/client-config.xml";
    initSparkContext(masterIp, appName);
    HHRDDHelper.initialize(clientConfigPath);
    hhrddInfo = HHRDDHelper.getHhrddInfo(hhFilePath);
    hhWithoutJobRDD = new HHRDD(context, hhrddInfo, false);
    sparkSession = SparkSession.builder().master(masterIp).appName(appName).getOrCreate();
    hhDSWithoutJobBuilder = new HHDatasetBuilder(hhWithoutJobRDD, hhrddInfo, sparkSession);

    // Column header is user defined.
    StructType schema = hhDSWithoutJobBuilder.createSchema(
        new String[] {"Col1", "Col2", "Col3", "Col4", "Col5", "Col6", "Col7", "Col8", "Col9"});
    JavaRDD<Row> rowRDD = hhWithoutJobRDD.toJavaRDD().map(new Function<byte[], Row>() {
      @Override
      public Row call(byte[] b) throws Exception {
        return hhDSWithoutJobBuilder.getRow(b);
      }
    });
    Dataset<Row> dataset = sparkSession.sqlContext().createDataFrame(rowRDD, schema);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = sparkSession
        .sql("SELECT * FROM TableView WHERE Col1 LIKE 'a' and Col2 LIKE 'b' and Col3 LIKE 'a' ");
    rs.show(false);

    // Simply changed the column header for same RDD.
    StructType schema1 = hhDSWithoutJobBuilder
        .createSchema(new String[] {"K1", "K2", "K3", "K4", "K5", "K6", "K7", "K8", "K9"});
    Dataset<Row> dataset1 = sparkSession.sqlContext().createDataFrame(rowRDD, schema1);
    dataset1.createOrReplaceTempView("TableView1");
    Dataset<Row> rs1 = sparkSession
        .sql("SELECT * FROM TableView1 WHERE K1 LIKE 'a' and K2 LIKE 'b' and K3 LIKE 'a' ");
    rs1.show(false);

    context.stop();
  }

  private static void initSparkContext(String masterIp, String appName) {
    if (context == null) {
      SparkConf conf = new SparkConf().setMaster(masterIp).setAppName(appName);
      context = new JavaSparkContext(conf);
    }
  }


}
