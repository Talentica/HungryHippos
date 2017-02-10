/**
 * 
 */
package com.talentica.hungryHippos.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;

import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.talentica.hungryHippos.rdd.HHBinaryRDD;
import com.talentica.hungryHippos.rdd.HHRDDInfo;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;
import com.talentica.hungryHippos.sql.HHSparkSession;
import com.talentica.hungryHippos.sql.HHStructField;
import com.talentica.hungryHippos.sql.HHStructType;

/**
 * @author pooshans
 *
 */
public class HHDataframeMain implements Serializable {

  private static final long serialVersionUID = 1230223941341399435L;
  private String masterIp;
  private String appName;
  private String hhFilePath;
  private String clientConfigPath;
  private String outputPath;
  private JavaSparkContext context;
  private HHRDDInfo hhrddInfo;
  private HHBinaryRDD hhDefaultRDD;
  private HHSparkSession hhSparkSession;

  public static void main(String[] args) throws FileNotFoundException, JAXBException,
      ClassNotFoundException, InstantiationException, IllegalAccessException {
    if (args.length < 5)
      throw new IllegalArgumentException("Invalid arguments");
    HHDataframeMain main = new HHDataframeMain();
    main.masterIp = args[0];
    main.appName = args[1];
    main.hhFilePath = args[2];
    main.clientConfigPath = args[3];
    main.outputPath = args[4];
    main.initSparkContext(main.masterIp, main.appName);
    HHRDDHelper.initialize(main.clientConfigPath);
    main.hhrddInfo = HHRDDHelper.getHhrddInfo(main.hhFilePath);
    main.hhDefaultRDD = new HHBinaryRDD(main.context, main.hhrddInfo, false);
    SparkSession sparkSession =
        SparkSession.builder().master(main.masterIp).appName(main.appName).getOrCreate();
    main.hhSparkSession =
        new HHSparkSession(sparkSession.sparkContext(), main.hhDefaultRDD, main.hhrddInfo);
    HHStructType hhStructType = new HHStructType();
    hhStructType.add(new HHStructField("col3", 2, true)).add(new HHStructField("col4", 3, true))
        .add(new HHStructField("col2", 1, true)).add(new HHStructField("col1", 0, true));
    main.hhSparkSession.addHHStructType(hhStructType);
    Dataset<Row> dataset = main.hhSparkSession.mapToDataset(TupleBean.class);
    dataset.createOrReplaceTempView("TableView");
    String sqlAggQuery = "SELECT col1,count(*) as sum FROM TableView GROUP BY col1";
    main.executeQuery(sqlAggQuery, "aggOut");
    /*
     * String sqlQuery =
     * "SELECT col1, col2, col3 FROM TableView WHERE col1 LIKE 'a' AND col2 LIKE 'b'";
     * main.executeQuery(sqlQuery, "out");
     */
  }

  private void executeQuery(String sqlText, String folder)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Dataset<Row> rs = this.hhSparkSession.sql(sqlText);
    rs.write().csv(outputPath + File.separatorChar + folder);
  }

  private void initSparkContext(String masterIp, String appName) {
    if (context == null) {
      SparkConf conf = new SparkConf().setMaster(masterIp).setAppName(appName);
      context = new JavaSparkContext(conf);
    }
  }

}
