/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import java.io.Serializable;

import javax.activation.UnsupportedDataTypeException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import com.talentica.hungryHippos.sql.HHSparkSession;

/**
 * @author pooshans
 *
 */
public abstract class HHRDDBuilder<T> implements Serializable {
  private static final long serialVersionUID = 1165414931806570599L;
  protected HHSparkSession<T> hhSparkSession;
  protected JavaRDD<T> javaRdd;

  public HHRDDBuilder(HHSparkSession<T> hhSparkSession) {
    this.hhSparkSession = hhSparkSession;
  }

  public HHRDDBuilder(JavaRDD<T> javaRdd, HHSparkSession<T> hhSparkSession) {
    this.javaRdd = javaRdd;
    this.hhSparkSession = hhSparkSession;
  }

  public abstract JavaRDD<Object> mapToJavaRDD(Class beanClazz);

  public abstract JavaRDD<Row> mapToJavaRDD();

  public abstract JavaRDD<Object> mapPartitionToJavaRDD(Class beanClazz);

  public abstract JavaRDD<Row> mapPartitionToJavaRDD();

  public abstract StructType getOrCreateSchema() throws UnsupportedDataTypeException;

  protected abstract Object getTuple(Class beanClazz) throws InstantiationException,
      IllegalAccessException, NoSuchFieldException, SecurityException;

  protected abstract Row getRow(T row) throws UnsupportedDataTypeException;
}
