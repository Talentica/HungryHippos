/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import org.apache.spark.api.java.JavaRDD;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.sql.HHSparkSession;

/**
 * Factory used to construct the object for data frame.
 * 
 * @author pooshans
 * @since 25/01/2017
 *
 */
public class HHDataframeFactory {

  public static <T> HHBinaryRDDBuilder<T> createHHBinaryJavaRDD(JavaRDD<T> javaRdd, FieldTypeArrayDataDescription description,
      HHSparkSession<T> hhSparkSession) {
    return new HHBinaryRDDBuilder<T>(javaRdd, description, hhSparkSession);
  }

  public static <T> HHBinaryRDDBuilder<T> createHHBinaryJavaRDD(FieldTypeArrayDataDescription description,
      HHSparkSession<T> hhSparkSession) {
    return new HHBinaryRDDBuilder<T>(description, hhSparkSession);
  }

  public static <T> HHTextRDDBuilder<T> createHHTextJavaRDD(JavaRDD<T> hhRdd,
      HHSparkSession<T> hhSparkSession) {
    return new HHTextRDDBuilder<T>(hhRdd, hhSparkSession);
  }

  public static <T> HHTextRDDBuilder<T> createHHTextJavaRDD(HHSparkSession<T> hhSparkSession) {
    return new HHTextRDDBuilder<T>(hhSparkSession);
  }

}
