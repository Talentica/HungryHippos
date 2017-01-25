/**
 * 
 */
package com.talentica.spark.test;

import java.io.FileNotFoundException;
import java.io.Serializable;

import javax.activation.UnsupportedDataTypeException;
import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.dataframe.HHDataframeFactory;
import com.talentica.hungryHippos.dataframe.HHDatasetBuilder;
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
  private HHDatasetBuilder hhDSWithoutJobBuilder;
  private HHDatasetBuilder hhDSWithJobBuilder;

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
    hhDSWithoutJobBuilder =
        HHDataframeFactory.createHHDataset(hhWithoutJobRDD, hhrddInfo, sparkSession);
    hhDSWithJobBuilder = HHDataframeFactory.createHHDataset(hhWithJobRDD, hhrddInfo, sparkSession);
  }

  @Test
  public void testDatasetForBeanByRowWiseWithJob() throws ClassNotFoundException {
    Dataset<Row> dataset = hhDSWithJobBuilder.mapToBeanDS(HHTuple.class);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = sparkSession
        .sql("SELECT * FROM TableView WHERE col1 LIKE 'a' and col2 LIKE 'b' and col3 LIKE 'a' ");
    rs.show(false);
    Assert.assertTrue(rs.count() > 0);
  }

  @Test
  public void testDatasetForBeanByRowWiseWithoutJob() throws ClassNotFoundException {
    Dataset<Row> dataset = hhDSWithoutJobBuilder.mapToBeanDS(HHTuple.class);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = sparkSession
        .sql("SELECT * FROM TableView WHERE col1 LIKE 'a' and col2 LIKE 'b' and col3 LIKE 'a' ");
    rs.show(false);
    Assert.assertTrue(rs.count() > 0);
  }

  @Test
  public void testDatasetForBeanByPartitionWithJob() throws ClassNotFoundException {
    Dataset<Row> dataset = hhDSWithJobBuilder.mapToBeanDS(HHTuple.class);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = sparkSession
        .sql("SELECT * FROM TableView WHERE col1 LIKE 'a' and col2 LIKE 'b' and col3 LIKE 'a' ");
    rs.show(false);
    Assert.assertTrue(rs.count() > 0);
  }

  @Test
  public void testDatasetForBeanByPartitionWithoutJob() throws ClassNotFoundException {
    Dataset<Row> dataset = hhDSWithoutJobBuilder.mapToBeanDS(HHTuple.class);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = sparkSession
        .sql("SELECT * FROM TableView WHERE col1 LIKE 'a' and col2 LIKE 'b' and col3 LIKE 'a' ");
    rs.show(false);
    Assert.assertTrue(rs.count() > 0);
  }

  @Test
  public void testStructTypeDatasetWithJob()
      throws UnsupportedDataTypeException, ClassNotFoundException {
    Dataset<Row> dataset = hhDSWithJobBuilder.mapToStructTypeDS(new String[] {"Column1", "Column2",
        "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9"});
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = sparkSession.sql(
        "SELECT * FROM TableView WHERE Column1 LIKE 'a' and Column2 LIKE 'b' and Column3 LIKE 'a' ");
    rs.show(false);
    Assert.assertTrue(rs.count() > 0);
  }

  @Test
  public void testStructTypeDatasetWithoutJob()
      throws UnsupportedDataTypeException, ClassNotFoundException {
    Dataset<Row> dataset = hhDSWithoutJobBuilder.mapToStructTypeDS(new String[] {"Column1",
        "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9"});
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = sparkSession.sql(
        "SELECT * FROM TableView WHERE Column1 LIKE 'a' and Column2 LIKE 'b' and Column3 LIKE 'a' ");
    rs.show(false);
    Assert.assertTrue(rs.count() > 0);
  }

  @Test
  public void testStructTypeDatasetWithJobForDifferentColumnName()
      throws UnsupportedDataTypeException, ClassNotFoundException {
    Dataset<Row> dataset = hhDSWithoutJobBuilder.mapToStructTypeDS(new String[] {"key1", "key2",
        "key3", "Column1", "Column2", "Column3", "Column4", "Column5", "Column6"});
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = sparkSession
        .sql("SELECT * FROM TableView WHERE key1 LIKE 'a' and key2 LIKE 'b' and key3 LIKE 'a' ");
    rs.show(false);
    Assert.assertTrue(rs.count() > 0);
  }

  @Test
  public void testHHJavaRDDAndHHDataset() throws UnsupportedDataTypeException {
    JavaRDD<Row> javaRDD = hhDSWithoutJobBuilder.mapToJavaRDD();
    StructType schema = hhDSWithoutJobBuilder.createSchema(new String[] {"key1", "key2", "key3",
        "Column1", "Column2", "Column3", "Column4", "Column5", "Column6"});
    Dataset<Row> dataset = sparkSession.sqlContext().createDataFrame(javaRDD, schema);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = sparkSession
        .sql("SELECT * FROM TableView WHERE key1 LIKE 'a' and key2 LIKE 'b' and key3 LIKE 'c' ");
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
