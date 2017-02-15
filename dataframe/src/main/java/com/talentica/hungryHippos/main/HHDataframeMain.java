/**
 * 
 */
package com.talentica.hungryHippos.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.activation.UnsupportedDataTypeException;
import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

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
  private HHSparkSession<byte[]> hhSparkSession;


  public static void main(String[] args)
      throws FileNotFoundException, JAXBException, ClassNotFoundException, InstantiationException,
      IllegalAccessException, UnsupportedDataTypeException {
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
    main.hhDefaultRDD = new HHBinaryRDD(main.context, main.hhrddInfo, true);
    SparkSession sparkSession =
        SparkSession.builder().master(main.masterIp).appName(main.appName).getOrCreate();
    main.hhSparkSession = new HHSparkSession<byte[]>(sparkSession.sparkContext(), main.hhrddInfo);
    HHStructType hhStructType = new HHStructType();
    hhStructType.add(new HHStructField("col0", 0, true, DataTypes.StringType))
        .add(new HHStructField("col1", 1, true, DataTypes.StringType))
        .add(new HHStructField("col2", 2, true, DataTypes.IntegerType))
        .add(new HHStructField("col3", 3, true, DataTypes.StringType))
        .add(new HHStructField("col4", 4, true, DataTypes.IntegerType))
        .add(new HHStructField("col5", 5, true, DataTypes.IntegerType));
    main.hhSparkSession.addHHStructType(hhStructType);
    int count = 0;

    for (String sqlQuery : getSqlQuery(main)) {
      Dataset<Row> dataset = main.hhSparkSession.mapRddToDataset(main.hhDefaultRDD);
      dataset.createOrReplaceTempView("TableView");
      main.executeQuery(sqlQuery, "out" + (count++));
    }
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

  private static List<String> getSqlQuery(HHDataframeMain main) {

    List<String> sqlQueries = new ArrayList<String>();
    sqlQueries.add("SELECT col0,sum(col4) as sum FROM TableView GROUP BY col0");
    sqlQueries.add("SELECT col0,sum(col5) as sum FROM TableView GROUP BY col0");

    sqlQueries.add("SELECT col0,col1,sum(col4) as sum FROM TableView GROUP BY col0,col1");
    sqlQueries.add("SELECT col0,col1,sum(col5) as sum FROM TableView GROUP BY col0,col1");

    sqlQueries.add("SELECT col0,col1,col2,sum(col4) as sum FROM TableView GROUP BY col0,col1,col2");
    sqlQueries.add("SELECT col0,col1,col2,sum(col5) as sum FROM TableView GROUP BY col0,col1,col2");

    sqlQueries.add("SELECT col0,col1,col3,sum(col4) as sum FROM TableView GROUP BY col0,col1,col3");
    sqlQueries.add("SELECT col0,col1,col3,sum(col5) as sum FROM TableView GROUP BY col0,col1,col3");

    sqlQueries.add("SELECT col0,col2,sum(col4) as sum FROM TableView GROUP BY col0,col2");
    sqlQueries.add("SELECT col0,col2,sum(col5) as sum FROM TableView GROUP BY col0,col2");

    sqlQueries.add("SELECT col0,col2,col3,sum(col4) as sum FROM TableView GROUP BY col0,col2,col3");
    sqlQueries.add("SELECT col0,col2,col3,sum(col5) as sum FROM TableView GROUP BY col0,col2,col3");

    sqlQueries.add("SELECT col0,col3,sum(col4) as sum FROM TableView GROUP BY col0,col3");
    sqlQueries.add("SELECT col0,col3,sum(col5) as sum FROM TableView GROUP BY col0,col3");

    sqlQueries.add("SELECT col1,sum(col4) as sum FROM TableView GROUP BY col1");
    sqlQueries.add("SELECT col1,sum(col5) as sum FROM TableView GROUP BY col1");

    sqlQueries.add("SELECT col1,col2,sum(col4) as sum FROM TableView GROUP BY col1,col2");
    sqlQueries.add("SELECT col1,col2,sum(col5) as sum FROM TableView GROUP BY col1,col2");

    sqlQueries.add("SELECT col1,col2,col3,sum(col4) as sum FROM TableView GROUP BY col1,col2,col3");
    sqlQueries.add("SELECT col1,col2,col3,sum(col5) as sum FROM TableView GROUP BY col1,col2,col3");

    sqlQueries.add("SELECT col1,col3,sum(col4) as sum FROM TableView GROUP BY col1,col3");
    sqlQueries.add("SELECT col1,col3,sum(col5) as sum FROM TableView GROUP BY col1,col3");

    sqlQueries.add("SELECT col2,sum(col4) as sum FROM TableView GROUP BY col2");
    sqlQueries.add("SELECT col2,sum(col5) as sum FROM TableView GROUP BY col2");

    sqlQueries.add("SELECT col2,col3,sum(col4) as sum FROM TableView GROUP BY col2,col3");
    sqlQueries.add("SELECT col2,col3,sum(col5) as sum FROM TableView GROUP BY col2,col3");

    System.out.println("total queries : " + sqlQueries.size());
    return sqlQueries;
  }

}
