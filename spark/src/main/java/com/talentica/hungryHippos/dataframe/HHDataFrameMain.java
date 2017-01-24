/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;

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
      NoSuchMethodException, InvocationTargetException, FileNotFoundException, JAXBException,
      UnsupportedDataTypeException {
    String masterIp = args[0];
    String appName = args[1];
    String hhFilePath = args[2];
    String clientConfigPath = args[3];
    initSparkContext(masterIp, appName);
    HHRDDHelper.initialize(clientConfigPath);
    HHRDDInfo hhrddInfo = HHRDDHelper.getHhrddInfo(hhFilePath);

    // RDD without any job. It loads all the partition in RDD.
    HHRDD hipposRDD = new HHRDD(context, hhrddInfo, false);

    // RDD with job. Selected loading of the partition based on job's nature.
    HHRDD hipposRDD1 = new HHRDD(context, hhrddInfo, new Integer[] {0}, false);
    SparkSession sparkSession =
        SparkSession.builder().master(masterIp).appName(appName).getOrCreate();

    HHDatasetConverter hhDatasetConverter =
        new HHDatasetConverter(hipposRDD, hhrddInfo, sparkSession);

    // Data set for struct type.
    Dataset<Row> dataset1 = hhDatasetConverter.toDatasetStructType(new String[] {"Column1",
        "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9"});
    dataset1.createOrReplaceTempView("TableView1");
    Dataset<Row> rs1 = sparkSession.sql(
        "SELECT * FROM TableView1 WHERE Column1 LIKE 'a' and Column2 LIKE 'b' and Column3 LIKE 'a' ");
    rs1.show(false);

    // Data set for row by row transformation internally.
    Dataset<Row> dataset2 = hhDatasetConverter.toDatasetByRow(HHTuple.class);
    dataset2.createOrReplaceTempView("TableView2");
    Dataset<Row> rs2 = sparkSession
        .sql("SELECT * FROM TableView2 WHERE key1 LIKE 'a' and key2 LIKE 'b' and key3 LIKE 'a' ");
    rs2.show(false);

    // Data set for row by row for each partition transformation internally.
    Dataset<Row> dataset3 = hhDatasetConverter.toDatasetByPartition(HHTuple.class);
    dataset3.createOrReplaceTempView("TableView3");
    Dataset<Row> rs3 = sparkSession
        .sql("SELECT * FROM TableView3 WHERE key1 LIKE 'a' and key2 LIKE 'b' and key3 LIKE 'a' ");
    rs3.show(false);

    // HHDatasetConverter for with job.
    HHDatasetConverter hhDatasetConverter1 =
        new HHDatasetConverter(hipposRDD1, hhrddInfo, sparkSession);
    Dataset<Row> dataset4 = hhDatasetConverter1.toDatasetStructType(new String[] {"Column1",
        "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9"});
    dataset4.createOrReplaceTempView("TableView4");
    Dataset<Row> rs4 = sparkSession.sql(
        "SELECT * FROM TableView4 WHERE Column1 LIKE 'a' and Column2 LIKE 'b' and Column3 LIKE 'a' ");
    rs4.show(false);


    // Client side exposure of the HH conversion API
    HHDatasetConverter hhDatasetConverter2 =
        new HHDatasetConverter(hipposRDD1, hhrddInfo, sparkSession);
    StructType schema = hhDatasetConverter2.createSchema(
        new String[] {"Col1", "Col2", "Col3", "Col4", "Col5", "Col6", "Col7", "Col8", "Col9"});
    JavaRDD<Row> rowRDD = hipposRDD1.toJavaRDD().map(new Function<byte[], Row>() {
      @Override
      public Row call(byte[] b) throws Exception {
        return hhDatasetConverter2.getRow(b);
      }
    });
    Dataset<Row> dataset5 = sparkSession.sqlContext().createDataFrame(rowRDD, schema);
    dataset5.createOrReplaceTempView("TableView5");
    Dataset<Row> rs5 = sparkSession
        .sql("SELECT * FROM TableView5 WHERE Col1 LIKE 'a' and Col2 LIKE 'b' and Col3 LIKE 'a' ");
    rs5.show(false);

    context.stop();

  }

  private static void initSparkContext(String masterIp, String appName) {
    if (context == null) {
      SparkConf conf = new SparkConf().setMaster(masterIp).setAppName(appName);
      context = new JavaSparkContext(conf);
    }
  }
}
