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
      HHSparkSession hhSparkSession) {
    return new HHBinaryRDDBuilder(hhRdd, hhrddInfo, hhSparkSession);
  }

  public static <T> HHBinaryRDDBuilder<T> createHHBinaryJavaRDD(HHRDDInfo hhrddInfo,
      HHSparkSession hhSparkSession) {
    return new HHBinaryRDDBuilder(hhrddInfo, hhSparkSession);
  }

  public static <T> HHTextRDDBuilder<T> createHHTextJavaRDD(HHRDD<T> hhRdd,
      HHSparkSession hhSparkSession) {
    return new HHTextRDDBuilder(hhRdd, hhSparkSession);
  }

  public static <T> HHTextRDDBuilder<T> createHHTextJavaRDD(HHSparkSession hhSparkSession) {
    return new HHTextRDDBuilder(hhSparkSession);
  }

}
