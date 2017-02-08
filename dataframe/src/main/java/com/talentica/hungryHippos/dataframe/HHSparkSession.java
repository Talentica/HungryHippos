/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import javax.activation.UnsupportedDataTypeException;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDInfo;

/**
 * @author pooshans
 *
 */
public class HHSparkSession extends SparkSession {
  private static final long serialVersionUID = -7510199519029156054L;
  private HHJavaRDDBuilder hhJavaRDDBuilder;
  private HHStructType hhStructType;

  public HHSparkSession(SparkContext sc, HHRDD hhRdd, HHRDDInfo hhrddInfo) {
    super(sc);
    this.hhJavaRDDBuilder = HHDataframeFactory.createHHJavaRDD(hhRdd, hhrddInfo, this);
    this.hhStructType = new HHStructType();
  }

  @Override
  public Dataset<Row> sql(String sqlText) {
    return super.sql(sqlText);
  }

  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row.
   * 
   * @param beanClazz
   * @return {@code Dataset<Row>}
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public <T> Dataset<Row> mapToDataset(Class<T> beanClazz)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    JavaRDD<T> rddDataframe = hhJavaRDDBuilder.mapToJavaRDD(beanClazz);
    Dataset<Row> dataset =
        sqlContext().createDataFrame(rddDataframe, Class.forName(beanClazz.getName()));
    return dataset;
  }


  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row. It is supported for schema definition of {@code StructType}.
   * 
   * @param fieldName : It is user defined column name. i.e new String[] {"Col1", "Col2", "Col3",
   *        "Col4", "Col5", "Col6", "Col7", "Col8", "Col9"}
   * @return {@code Dataset<Row>}
   * @throws ClassNotFoundException
   * @throws UnsupportedDataTypeException
   */
  public Dataset<Row> mapToDataset() throws ClassNotFoundException, UnsupportedDataTypeException {
    StructType schema = hhJavaRDDBuilder.getOrCreateSchema();
    JavaRDD<Row> rowRDD = hhJavaRDDBuilder.mapToJavaRDD();
    Dataset<Row> dataset = sqlContext().createDataFrame(rowRDD, schema);
    return dataset;
  }

  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row. It is supported for schema definition of {@code StructType}.
   * 
   * @param schema
   * @return {@code Dataset<Row>}
   * @throws ClassNotFoundException
   * @throws UnsupportedDataTypeException
   */
  public Dataset<Row> mapToDataset(StructType schema)
      throws ClassNotFoundException, UnsupportedDataTypeException {
    JavaRDD<Row> rowRDD = hhJavaRDDBuilder.mapToJavaRDD();
    return sqlContext().createDataFrame(rowRDD, schema);
  }


  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row for each partition.
   * 
   * @param beanClazz
   * @return {@code Dataset<Row>}
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public <T> Dataset<Row> mapPartitionToDataset(Class<T> beanClazz)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    JavaRDD<T> rddDataframe = hhJavaRDDBuilder.mapPartitionToJavaRDD(beanClazz);
    Dataset<Row> dataset =
        sqlContext().createDataFrame(rddDataframe, Class.forName(beanClazz.getName()));
    return dataset;
  }

  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row for each partition. It is supported for schema definition of {@code StructType}.
   * 
   * @param fieldName : It is user defined column name. i.e new String[] {"Col1", "Col2", "Col3",
   *        "Col4", "Col5", "Col6", "Col7", "Col8", "Col9"}
   * @return {@code Dataset<Row>}
   * @throws ClassNotFoundException
   * @throws UnsupportedDataTypeException
   */
  public Dataset<Row> mapPartitionToDataset()
      throws ClassNotFoundException, UnsupportedDataTypeException {
    StructType schema = hhJavaRDDBuilder.getOrCreateSchema();
    JavaRDD<Row> rowRDD = hhJavaRDDBuilder.mapPartitionToJavaRDD();
    return sqlContext().createDataFrame(rowRDD, schema);
  }

  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row for each partition. It is supported for schema definition of {@code StructType}.
   * 
   * @param schema
   * @return {@code Dataset<Row>}
   * @throws ClassNotFoundException
   * @throws UnsupportedDataTypeException
   */
  public Dataset<Row> mapPartitionToDataset(StructType schema)
      throws ClassNotFoundException, UnsupportedDataTypeException {
    JavaRDD<Row> rowRDD = hhJavaRDDBuilder.mapPartitionToJavaRDD();
    return sqlContext().createDataFrame(rowRDD, schema);
  }

  public void addHHStructType(HHStructType hhStructType) {
    this.hhStructType = hhStructType;
  }

  protected HHStructType getHHStructType() {
    return this.hhStructType;
  }

  public void start() {
    if (!hhStructType.isEmpty()) {
      hhStructType.clear();
    }
  }

  /**
   * It toggle the status of the column. If the status of the column is false it makes it true and
   * vice-versa.
   * 
   * @param columnName
   */
  public void toggleHHStructFieldStatus(String columnName) {
    hhStructType.toggleColumnStatus(columnName);
  }

  public void end() {
    hhStructType.clear();
  }

}
