/**
 * 
 */
package com.talentica.spark.test;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.nio.ByteBuffer;

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

import com.foundationdb.sql.StandardException;
import com.talentica.hungryHippos.dataframe.HHSparkSession;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDInfo;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;

/**
 * @author pooshans
 * @since 25/01/2017
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
  private HHSparkSession hhWithoutJobSparkSession;
  private HHSparkSession hhWithJobSparkSession;

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
    SparkSession sparkSession = SparkSession.builder().master(masterIp).appName(appName).getOrCreate();
    hhWithoutJobSparkSession =
        new HHSparkSession(sparkSession.sparkContext(), hhWithoutJobRDD, hhrddInfo);
    hhWithJobSparkSession =
        new HHSparkSession(sparkSession.sparkContext(), hhWithJobRDD, hhrddInfo);
  }

  @Test
  public void testDatasetForBeanByRowWiseWithJob()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Dataset<Row> dataset = hhWithJobSparkSession.mapToDataset(TupleBean.class);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = hhWithJobSparkSession
        .sql("SELECT col1,col4 FROM TableView WHERE col1 LIKE 'a' and col2 LIKE 'b' and col3 LIKE 'a' ");
    rs.show(false);
    Assert.assertTrue(rs.count() > 0);
  }

  @Test
  public void testDatasetForBeanByRowWiseWithoutJob()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Dataset<Row> dataset = hhWithoutJobSparkSession.mapToDataset(TupleBean.class);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = hhWithoutJobSparkSession
        .sql("SELECT * FROM TableView WHERE col1 LIKE 'a' and col2 LIKE 'b' and col3 LIKE 'a' ");
    rs.show(false);
    Assert.assertTrue(rs.count() > 0);
  }

  @Test
  public void testDatasetForBeanByPartitionWithJob()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Dataset<Row> dataset = hhWithJobSparkSession.mapToDataset(TupleBean.class);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = hhWithJobSparkSession
        .sql("SELECT * FROM TableView WHERE col1 LIKE 'a' and col2 LIKE 'b' and col3 LIKE 'a' ");
    rs.show(false);
    Assert.assertTrue(rs.count() > 0);
  }

  @Test
  public void testDatasetForBeanByPartitionWithoutJob()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Dataset<Row> dataset = hhWithoutJobSparkSession.mapToDataset(TupleBean.class);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = hhWithoutJobSparkSession
        .sql("SELECT * FROM TableView WHERE col1 LIKE 'a' and col2 LIKE 'b' and col3 LIKE 'a' ");
    rs.show(false);
    Assert.assertTrue(rs.count() > 0);
  }

  @Test
  public void testStructTypeDatasetWithJob()
      throws UnsupportedDataTypeException, ClassNotFoundException, StandardException {
    Dataset<Row> dataset = hhWithJobSparkSession.mapToDataset(
        new String[] {"Column1", "Column2", "Column3", null, null, null, null, null, null});
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = hhWithJobSparkSession.sql(
        "SELECT Column1, Column2,Column3 FROM TableView WHERE Column1 LIKE 'a' and Column2 LIKE 'b' and Column3 LIKE 'a' ");
    rs.show(false);
    Assert.assertTrue(rs.count() > 0);
  }

  @Test
  public void testStructTypeDatasetWithoutJob()
      throws UnsupportedDataTypeException, ClassNotFoundException {
    Dataset<Row> dataset = hhWithoutJobSparkSession.mapToDataset(
        new String[] {"Column1", "Column2", "Column3", null, null, null, null, null, null});
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = hhWithoutJobSparkSession.sql(
        "SELECT Column1 FROM TableView WHERE Column1 LIKE 'a' and Column2 LIKE 'b' and Column3 LIKE 'a' ");
    rs.show(false);
    Assert.assertTrue(rs.count() > 0);
  }

  @Test
  public void testStructTypeDatasetWithJobForDifferentColumnName()
      throws UnsupportedDataTypeException, ClassNotFoundException, StandardException {
    Dataset<Row> dataset = hhWithoutJobSparkSession.mapToDataset(
        new String[] {"key1", "key2", "key3", null, null, null, "Column4", null, null});
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = hhWithoutJobSparkSession.sql(
        "SELECT  Column4 FROM TableView WHERE key1 LIKE 'a' and key2 LIKE 'b' and key3 LIKE 'a' ");
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

  public static void main(String[] args) {
    byte[] b = new byte[] {12, 13};
    ByteBuffer buffer = ByteBuffer.wrap(b);
    System.out.println(buffer.get());
    System.out.println(buffer.get());
    b[0] = 15;
    b[1] = 16;
    buffer.flip();
    System.out.println(buffer.get());
    System.out.println(buffer.get());
  }
}
