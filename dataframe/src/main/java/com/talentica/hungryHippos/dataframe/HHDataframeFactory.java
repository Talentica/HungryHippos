/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDInfo;
import com.talentica.hungryHippos.sql.HHSparkSession;

/**
 * Factory used to construct the object for data frame.
 * 
 * @author pooshans
 * @since 25/01/2017
 *
 */
public class HHDataframeFactory {

  public static <T> HHBinaryRDDBuilder<T> createHHBinaryJavaRDD(HHRDD<T> hhRdd, HHRDDInfo hhrddInfo,
      HHSparkSession<T> hhSparkSession) {
    return new HHBinaryRDDBuilder<T>(hhRdd, hhrddInfo, hhSparkSession);
  }

  public static <T> HHBinaryRDDBuilder<T> createHHBinaryJavaRDD(HHRDDInfo hhrddInfo,
      HHSparkSession<T> hhSparkSession) {
    return new HHBinaryRDDBuilder<T>(hhrddInfo, hhSparkSession);
  }

  public static <T> HHTextRDDBuilder<T> createHHTextJavaRDD(HHRDD<T> hhRdd,
      HHSparkSession<T> hhSparkSession) {
    return new HHTextRDDBuilder<T>(hhRdd, hhSparkSession);
  }

  public static <T> HHTextRDDBuilder<T> createHHTextJavaRDD(HHSparkSession<T> hhSparkSession) {
    return new HHTextRDDBuilder<T>(hhSparkSession);
  }

}
