/**
 * 
 */
package com.talentica.hungryHippos.sql;

import javax.activation.UnsupportedDataTypeException;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import com.talentica.hungryHippos.dataframe.HHDataframeFactory;
import com.talentica.hungryHippos.dataframe.HHRDDBuilder;
import com.talentica.hungryHippos.rdd.HHBinaryRDD;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDInfo;
import com.talentica.hungryHippos.rdd.HHTextRDD;

/**
 * @author pooshans
 * @param <T>
 * @param <T>
 *
 */
public class HHSparkSession<T> extends SparkSession {
  private static final long serialVersionUID = -7510199519029156054L;
  private HHStructType hhStructType;
  private HHRDDInfo hhrddInfo;

  public HHSparkSession(SparkContext sc, HHRDDInfo hhrddInfo) {
    super(sc);
    this.hhrddInfo = hhrddInfo;
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
  public Dataset<Row> mapRDDToDataset(HHRDD<T> hhRdd, Class<?> beanClazz)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    HHRDDBuilder<T> hhJavaRDDBuilder = getRDD(hhRdd);
    JavaRDD<Object> rddDataframe = hhJavaRDDBuilder.mapToJavaRDD(beanClazz);
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
  public Dataset<Row> mapRddToDataset(HHRDD<T> hhRdd)
      throws ClassNotFoundException, UnsupportedDataTypeException {
    HHRDDBuilder<T> hhJavaRDDBuilder = getRDD(hhRdd);
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
  public Dataset<Row> mapRDDToDataset(HHRDD<T> hhRdd, StructType schema)
      throws ClassNotFoundException, UnsupportedDataTypeException {
    HHRDDBuilder<T> hhJavaRDDBuilder = getRDD(hhRdd);
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
  public Dataset<Row> mapPartitionRDDToDataset(HHRDD<T> hhRdd, Class<?> beanClazz)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    HHRDDBuilder<T> hhJavaRDDBuilder = getRDD(hhRdd);
    JavaRDD<Object> rddDataframe = hhJavaRDDBuilder.mapPartitionToJavaRDD(beanClazz);
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
  public Dataset<Row> mapPartitionRDDToDataset(HHRDD<T> hhRdd)
      throws ClassNotFoundException, UnsupportedDataTypeException {
    HHRDDBuilder<T> hhJavaRDDBuilder = getRDD(hhRdd);
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
  public Dataset<Row> mapPartitionRDDToDataset(HHRDD<T> hhRdd, StructType schema)
      throws ClassNotFoundException, UnsupportedDataTypeException {
    HHRDDBuilder<T> hhJavaRDDBuilder = getRDD(hhRdd);
    JavaRDD<Row> rowRDD = hhJavaRDDBuilder.mapPartitionToJavaRDD();
    return sqlContext().createDataFrame(rowRDD, schema);
  }

  public void addHHStructType(HHStructType hhStructType) {
    this.hhStructType = hhStructType;
  }

  public HHStructType getHHStructType() {
    return this.hhStructType;
  }


  /**
   * It toggle the status of the column. If the status of the column is false it makes it true and
   * vice-versa.
   * 
   * @param columnsName
   */
  public void toggleHHStructFieldStatus(String[] columnsName) {
    hhStructType.toggleColumnStatus(columnsName);
  }

  private HHRDDBuilder<T> getRDD(HHRDD<T> hhRdd) {
    if (hhRdd instanceof HHBinaryRDD) {
      return (HHRDDBuilder<T>) HHDataframeFactory.createHHBinaryJavaRDD(hhRdd, hhrddInfo, this);
    } else if (hhRdd instanceof HHTextRDD) {
      return (HHRDDBuilder<T>) HHDataframeFactory.createHHTextJavaRDD(hhRdd, this);
    }
    return null;
  }

}
