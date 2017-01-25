/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import java.io.Serializable;

import javax.activation.UnsupportedDataTypeException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDInfo;

/**
 * To build the data set for given HungryHippos RDD which convert the underlying HH data
 * representation to the Spark recognized data format.
 * 
 * @author pooshans
 * @since 25/01/2017
 * @param <T>
 *
 */
public class HHDatasetBuilder extends HHJavaRDDBuilder implements Serializable {
  private static final long serialVersionUID = 3564747948683195956L;

  /**
   * Constructor of {@code HHDatasetConverter} which is instantiated if client requires prebuild
   * {@code Dataset}.
   * 
   * @param hhRdd
   * @param hhrddInfo
   * @param sparkSession
   */
  public HHDatasetBuilder(HHRDD hhRdd, HHRDDInfo hhrddInfo, SparkSession sparkSession) {
    super(hhRdd, hhrddInfo, sparkSession);
  }

  /**
   * Constructor of {@code HHDatasetConverter} which is required to only if client need utility such
   * as {@link #getRow(byte[])} or {@link #createSchema(String[])}
   * 
   * @param hhrddInfo
   */
  public HHDatasetBuilder(HHRDDInfo hhrddInfo) {
    super(hhrddInfo);
  }


  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row.
   * 
   * @param beanClazz
   * @return {@code Dataset<Row>}
   * @throws ClassNotFoundException
   */
  public <T> Dataset<Row> mapToDataset(Class<T> beanClazz) throws ClassNotFoundException {
    JavaRDD<T> rddDataframe = mapToJavaRDD(beanClazz);
    Dataset<Row> dataset =
        sparkSession.sqlContext().createDataFrame(rddDataframe, Class.forName(beanClazz.getName()));
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
  public Dataset<Row> mapToDataset(String[] fieldName)
      throws ClassNotFoundException, UnsupportedDataTypeException {
    StructType schema = createSchema(fieldName);
    JavaRDD<Row> rowRDD = mapToJavaRDD();
    return sparkSession.sqlContext().createDataFrame(rowRDD, schema);
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
    JavaRDD<Row> rowRDD = mapToJavaRDD();
    return sparkSession.sqlContext().createDataFrame(rowRDD, schema);
  }


  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row for each partition.
   * 
   * @param beanClazz
   * @return {@code Dataset<Row>}
   * @throws ClassNotFoundException
   */
  public <T> Dataset<Row> mapPartitionToDataset(Class<T> beanClazz) throws ClassNotFoundException {
    JavaRDD<T> rddDataframe = mapPartitionToJavaRDD(beanClazz);
    Dataset<Row> dataset =
        sparkSession.sqlContext().createDataFrame(rddDataframe, Class.forName(beanClazz.getName()));
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
  public Dataset<Row> mapPartitionToDataset(String[] fieldName)
      throws ClassNotFoundException, UnsupportedDataTypeException {
    StructType schema = createSchema(fieldName);
    JavaRDD<Row> rowRDD = mapPartitionToJavaRDD();
    return sparkSession.sqlContext().createDataFrame(rowRDD, schema);
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
    JavaRDD<Row> rowRDD = mapPartitionToJavaRDD();
    return sparkSession.sqlContext().createDataFrame(rowRDD, schema);
  }

}
