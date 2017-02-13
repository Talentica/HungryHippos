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

  public static <T> HHBinaryRDDBuilder<T> createHHJavaRDD(HHRDD<T> hhRdd, HHRDDInfo hhrddInfo,
      HHSparkSession hhSparkSession) {
    return new HHBinaryRDDBuilder(hhRdd, hhrddInfo, hhSparkSession);
  }

  public static <T> HHBinaryRDDBuilder<T> createHHJavaRDD(HHRDDInfo hhrddInfo,
      HHSparkSession hhSparkSession) {
    return new HHBinaryRDDBuilder(hhrddInfo, hhSparkSession);
  }

}
