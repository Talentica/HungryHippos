/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;

import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDInfo;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;

/**
 * @author pooshans
 *
 */
public class HHDataFrameMain {

  private static JavaSparkContext context;

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
    SparkSession sparkSession =
        SparkSession.builder().master(masterIp).appName(appName).getOrCreate();

    HHDataset hhDataset = new HHDataset(hipposRDD, hhrddInfo, sparkSession);

    // Data set for row by row transformation internally.
    Dataset<Row> dataset = hhDataset.toDatasetByRow(HHTuple.class);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = sparkSession
        .sql("SELECT * FROM TableView WHERE key1 LIKE 'a' and key2 LIKE 'b' and key3 LIKE 'a' ");
    rs.show(false);

    // Data set for row by row for each partition transformation internally.
    Dataset<Row> dataset1 = hhDataset.toDatasetByRow(HHTuple.class);
    dataset1.createOrReplaceTempView("TableView1");
    Dataset<Row> rs1 = sparkSession
        .sql("SELECT * FROM TableView1 WHERE key1 LIKE 'a' and key2 LIKE 'b' and key3 LIKE 'a' ");
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
