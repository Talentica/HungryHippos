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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import com.talentica.hdfs.spark.binary.job.DataDescriptionConfig;
import com.talentica.hungryHippos.sql.HHSparkSession;
import com.talentica.hungryHippos.sql.HHStructField;
import com.talentica.hungryHippos.sql.HHStructType;

/**
 * @author pooshans
 *
 */
public class HadoopDataframeMain implements Serializable {

  private static final long serialVersionUID = 6477569421350448245L;
  private static JavaSparkContext context;
  @SuppressWarnings("rawtypes")
  private static HHSparkSession hhSparkSession;
  private static String outputDir;

  public static void main(String[] args)
      throws FileNotFoundException, JAXBException, ClassNotFoundException, InstantiationException,
      IllegalAccessException, UnsupportedDataTypeException {
    if (args.length < 5)
      throw new IllegalArgumentException("Invalid arguments");
    HadoopDataframeMain main = new HadoopDataframeMain();
    String masterIp = args[0];
    String appName = args[1];
    String inputFile = args[2];
    outputDir = args[3];
    String shardingFolderPath = args[4];
    main.initSparkContext(masterIp, appName);
    SparkSession sparkSession =
        SparkSession.builder().master(masterIp).appName(appName).getOrCreate();
    DataDescriptionConfig dataDescriptionConfig = new DataDescriptionConfig(shardingFolderPath);
    JavaRDD<byte[]> rdd = context.binaryRecords(inputFile, dataDescriptionConfig.getRowSize());

    hhSparkSession = new HHSparkSession<byte[]>(sparkSession.sparkContext(),
        dataDescriptionConfig.getDataDescription());
    HHStructType hhStructType = new HHStructType();
    hhStructType.add(new HHStructField("col0", 0, true, DataTypes.StringType))
        .add(new HHStructField("col1", 1, true, DataTypes.StringType))
        .add(new HHStructField("col2", 2, true, DataTypes.IntegerType))
        .add(new HHStructField("col3", 3, true, DataTypes.StringType))
        .add(new HHStructField("col4", 4, true, DataTypes.IntegerType))
        .add(new HHStructField("col5", 5, true, DataTypes.IntegerType));
    hhSparkSession.addHHStructType(hhStructType);
    int count = 0;
    for (String sqlQuery : getSqlQuery()) {
      @SuppressWarnings("unchecked")
      Dataset<Row> dataset = hhSparkSession.mapRDDToDataset(rdd.rdd());
      dataset.createOrReplaceTempView("TableView");
      main.executeQuery(sqlQuery, "out" + (count++));
    }
  }

  private void executeQuery(String sqlText, String folder)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    @SuppressWarnings("unchecked")
    Dataset<Row> rs = hhSparkSession.sql(sqlText);
    rs.write().csv(outputDir + File.separatorChar + folder);
  }

  private void initSparkContext(String masterIp, String appName) {
    if (context == null) {
      SparkConf conf = new SparkConf().setMaster(masterIp).setAppName(appName);
      context = new JavaSparkContext(conf);
    }
  }

  private static List<String> getSqlQuery() {

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
