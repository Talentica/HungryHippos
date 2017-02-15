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
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.main.TupleBean;
import com.talentica.hungryHippos.rdd.HHRDDInfo;
import com.talentica.hungryHippos.rdd.HHTextRDD;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;
import com.talentica.hungryHippos.sql.HHSparkSession;
import com.talentica.hungryHippos.sql.HHStructField;
import com.talentica.hungryHippos.sql.HHStructType;

/**
 * @author pooshans
 *
 */
public class HHDataframeOnTextFileSystemTest implements Serializable {

  private static final long serialVersionUID = -602425589870050732L;
  private String masterIp;
  private String appName;
  private String hhFilePath;
  private String clientConfigPath;
  private JavaSparkContext context;
  private HHRDDInfo hhrddInfo;
  private HHTextRDD hhDefaultRDD;
  private HHTextRDD hhJobRDD;
  private HHSparkSession<String> hhSparkSession;
  private HHSparkSession<String> hhJobSparkSession;

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
    hhDefaultRDD = new HHTextRDD(context, hhrddInfo, false);
    hhJobRDD = new HHTextRDD(context, hhrddInfo, new Integer[] {0}, false);
    SparkSession sparkSession =
        SparkSession.builder().master(masterIp).appName(appName).getOrCreate();
    hhSparkSession =
        new HHSparkSession<String>(sparkSession.sparkContext(), hhrddInfo.getFieldDataDesc());
    hhJobSparkSession =
        new HHSparkSession<String>(sparkSession.sparkContext(), hhrddInfo.getFieldDataDesc());
  }

  @Test
  public void testDatasetGroupByForBeanWithJob()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    HHStructType hhStructType = new HHStructType();
    hhStructType.add(new HHStructField("col3", 2, true, DataTypes.StringType))
        .add(new HHStructField("col4", 3, true, DataTypes.IntegerType))
        .add(new HHStructField("col2", 1, true, DataTypes.StringType))
        .add(new HHStructField("col1", 0, true, DataTypes.StringType));
    hhJobSparkSession.addHHStructType(hhStructType);
    Dataset<Row> dataset = hhJobSparkSession.mapRDDToDataset(hhJobRDD, TupleBean.class);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = hhJobSparkSession.sql("SELECT sum(col4) as sum FROM TableView GROUP BY col1");
    rs.show(false);
    Assert.assertNotNull(rs);
  }

  @Test
  public void testDatasetForBeanByRowWiseWithoutJob()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    HHStructType hhStructType = new HHStructType();
    hhStructType.add(new HHStructField("col1", 0, true, DataTypes.StringType))
        .add(new HHStructField("col2", 1, true, DataTypes.StringType))
        .add(new HHStructField("col3", 2, true, DataTypes.StringType));
    hhSparkSession.addHHStructType(hhStructType);
    Dataset<Row> dataset = hhSparkSession.mapRDDToDataset(hhDefaultRDD, TupleBean.class);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = hhSparkSession.sql(
        "SELECT col1,col2,col3 FROM TableView WHERE col1 LIKE 'a' and col2 LIKE 'b' and col3 LIKE 'a' ");
    rs.show(false);
    Assert.assertNotNull(rs);
  }

  @Test
  public void testDatasetForBeanByPartitionWithJob()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    HHStructType hhStructType = new HHStructType();
    hhStructType.add(new HHStructField("col1", 0, true, DataTypes.StringType))
        .add(new HHStructField("col2", 1, true, DataTypes.StringType))
        .add(new HHStructField("col3", 2, true, DataTypes.StringType));
    hhJobSparkSession.addHHStructType(hhStructType);
    Dataset<Row> dataset = hhJobSparkSession.mapRDDToDataset(hhJobRDD, TupleBean.class);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = hhJobSparkSession.sql(
        "SELECT col1,col2,col3 FROM TableView WHERE col1 LIKE 'a' and col2 LIKE 'b' and col3 LIKE 'a' ");
    rs.show(false);
    Assert.assertNotNull(rs);
  }

  @Test
  public void testDatasetForBeanByPartitionWithoutJob()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    HHStructType hhStructType = new HHStructType();
    hhStructType.add(new HHStructField("col1", 0, true, DataTypes.StringType))
        .add(new HHStructField("col2", 1, true, DataTypes.StringType))
        .add(new HHStructField("col3", 2, true, DataTypes.StringType))
        .add(new HHStructField("col5", 4, true, DataTypes.IntegerType));
    hhSparkSession.addHHStructType(hhStructType);
    Dataset<Row> dataset = hhSparkSession.mapRDDToDataset(hhDefaultRDD, TupleBean.class);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = hhSparkSession.sql(
        "SELECT col1,col2,col5 FROM TableView WHERE col1 LIKE 'a' and col2 LIKE 'b' and col3 LIKE 'a' ");
    rs.show(false);
    Assert.assertNotNull(rs);
  }

  @Test
  public void testStructTypeDatasetWithJob()
      throws UnsupportedDataTypeException, ClassNotFoundException {

    HHStructType hhStructType = new HHStructType();
    hhStructType.add(new HHStructField("Column1", 0, true, DataTypes.StringType))
        .add(new HHStructField("Column2", 1, true, DataTypes.StringType))
        .add(new HHStructField("Column3", 2, true, DataTypes.StringType))
        .add(new HHStructField("Column4", 3, false, DataTypes.IntegerType));

    hhJobSparkSession.addHHStructType(hhStructType);
    Dataset<Row> dataset = hhJobSparkSession.mapRDDToDataset(hhJobRDD);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = hhJobSparkSession.sql(
        "SELECT Column1, Column2,Column3 FROM TableView WHERE Column1 LIKE 'a' and Column2 LIKE 'b' and Column3 LIKE 'a' ");
    rs.show(false);
    hhJobSparkSession.toggleHHStructFieldStatus(new String[] {"Column3"});
    dataset.createOrReplaceTempView("TableView1");
    Dataset<Row> rs1 = hhJobSparkSession
        .sql("SELECT Column1, Column2 FROM TableView1 WHERE Column1 LIKE 'a' and Column2 LIKE 'b'");
    rs1.show(false);
    Assert.assertNotNull(rs1);
  }

  @Test
  public void testStructTypeDatasetWithoutJob()
      throws UnsupportedDataTypeException, ClassNotFoundException {

    HHStructType hhStructType = new HHStructType();
    hhStructType.add(new HHStructField("Column1", 0, true, DataTypes.StringType))
        .add(new HHStructField("Column2", 1, true, DataTypes.StringType))
        .add(new HHStructField("Column3", 2, true, DataTypes.StringType));
    hhSparkSession.addHHStructType(hhStructType);

    Dataset<Row> dataset = hhSparkSession.mapRDDToDataset(hhDefaultRDD);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = hhSparkSession.sql(
        "SELECT Column1 FROM TableView WHERE Column1 LIKE 'a' and Column2 LIKE 'b' and Column3 LIKE 'a' ");
    rs.show(false);
    Assert.assertNotNull(rs);
  }

  @Test
  public void testStructTypeDatasetWithJobForDifferentColumnName()
      throws UnsupportedDataTypeException, ClassNotFoundException {

    HHStructType hhStructType = new HHStructType();
    hhStructType.add(new HHStructField("key1", 0, true, DataTypes.StringType))
        .add(new HHStructField("key2", 1, true, DataTypes.StringType))
        .add(new HHStructField("key3", 2, true, DataTypes.StringType))
        .add(new HHStructField("Column4", 3, true, DataTypes.StringType));
    hhSparkSession.addHHStructType(hhStructType);

    Dataset<Row> dataset = hhSparkSession.mapRDDToDataset(hhDefaultRDD);
    dataset.createOrReplaceTempView("TableView");
    Dataset<Row> rs = hhSparkSession.sql(
        "SELECT  Column4 FROM TableView WHERE key1 LIKE 'a' and key2 LIKE 'b' and key3 LIKE 'a' ");
    rs.show(false);
    Assert.assertNotNull(rs);
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
