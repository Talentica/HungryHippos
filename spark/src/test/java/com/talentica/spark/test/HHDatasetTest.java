/**
 * 
 */
package com.talentica.spark.test;

import java.io.FileNotFoundException;
import java.io.Serializable;

import javax.activation.UnsupportedDataTypeException;
import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.dataframe.HHDatasetConverter;
import com.talentica.hungryHippos.dataframe.HHTuple;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDInfo;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;

/**
 * @author pooshans
 *
 */
public class HHDatasetTest implements Serializable {
  private static final long serialVersionUID = -602425589870050732L;
  private String masterIp;
  private String appName;
  private String hhFilePath;
  private String clientConfigPath;
  private JavaSparkContext context;
  private HHRDDInfo hhrddInfo;
  private HHRDD hhWithoutJobRDD;
  private HHRDD hhWithJobRDD;
  private SparkSession sparkSession;
  private HHDatasetConverter hhWithoutJobDC;
  private HHDatasetConverter hhWithJobDC;

  @Before
  public void setUp() throws FileNotFoundException, JAXBException, UnsupportedDataTypeException,
      ClassNotFoundException {
    masterIp = "local[*]";
    appName = "testApp";
    hhFilePath = "/distr/data";
    clientConfigPath =
        "/home/pooshans/HungryHippos/HungryHippos/configuration-schema/src/main/resources/distribution/client-config.xml";
    initSparkContext(masterIp, appName);
    HHRDDHelper.initialize(clientConfigPath);
    hhrddInfo = HHRDDHelper.getHhrddInfo(hhFilePath);
    hhWithoutJobRDD = new HHRDD(context, hhrddInfo, false);
    hhWithJobRDD = new HHRDD(context, hhrddInfo, new Integer[] {0}, false);
    sparkSession = SparkSession.builder().master(masterIp).appName(appName).getOrCreate();
    hhWithoutJobDC = new HHDatasetConverter(hhWithoutJobRDD, hhrddInfo, sparkSession);
    hhWithJobDC = new HHDatasetConverter(hhWithJobRDD, hhrddInfo, sparkSession);
  }

  @Test
  public void testDatasetForBeanWithJob() throws ClassNotFoundException {
    Dataset<Row> dataset = hhWithJobDC.toDatasetByRow(HHTuple.class);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = sparkSession
        .sql("SELECT * FROM TableView WHERE key1 LIKE 'a' and key2 LIKE 'b' and key3 LIKE 'a' ");
    rs.show(false);
    Assert.assertTrue(rs.count() > 0);
  }

  @Test
  public void testDatasetForBeanWithoutJob() throws ClassNotFoundException {
    Dataset<Row> dataset = hhWithoutJobDC.toDatasetByRow(HHTuple.class);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = sparkSession
        .sql("SELECT * FROM TableView WHERE key1 LIKE 'a' and key2 LIKE 'b' and key3 LIKE 'a' ");
    rs.show(false);
    Assert.assertTrue(rs.count() > 0);
  }

  @Test
  public void testStructTypeDatasetWithJob() throws UnsupportedDataTypeException {
    Dataset<Row> dataset = hhWithJobDC.toDatasetStructType(new String[] {"Column1", "Column2",
        "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9"});
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = sparkSession.sql(
        "SELECT * FROM TableView WHERE Column1 LIKE 'a' and Column2 LIKE 'b' and Column3 LIKE 'a' ");
    rs.show(false);
    Assert.assertTrue(rs.count() > 0);
  }

  @Test
  public void testStructTypeDatasetWithoutJob() throws UnsupportedDataTypeException {
    Dataset<Row> dataset = hhWithoutJobDC.toDatasetStructType(new String[] {"Column1", "Column2",
        "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9"});
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = sparkSession.sql(
        "SELECT * FROM TableView WHERE Column1 LIKE 'a' and Column2 LIKE 'b' and Column3 LIKE 'a' ");
    rs.show(false);
    Assert.assertTrue(rs.count() > 0);
  }

  @Test
  public void testStructTypeDatasetWithJobForDifferentColumnName()
      throws UnsupportedDataTypeException {
    Dataset<Row> dataset = hhWithoutJobDC.toDatasetStructType(new String[] {"key1", "key2", "key3",
        "Column1", "Column2", "Column3", "Column4", "Column5", "Column6"});
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = sparkSession
        .sql("SELECT * FROM TableView WHERE key1 LIKE 'a' and key2 LIKE 'b' and key3 LIKE 'a' ");
    rs.show(false);
    Assert.assertTrue(rs.count() > 0);
  }


  private void initSparkContext(String masterIp, String appName) {
    if (context == null) {
      SparkConf conf = new SparkConf().setMaster(masterIp).setAppName(appName);
      context = new JavaSparkContext(conf);
    }
  }

  @After
  public void tearDown() {
    context.stop();
  }

}
