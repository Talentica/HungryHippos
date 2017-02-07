/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDInfo;

/**
 * Factory used to construct the object for data frame.
 * 
 * @author pooshans
 * @since 25/01/2017
 *
 */
public class HHDataframeFactory {

  public static HHDatasetBuilder createHHDataset(HHRDD hhRdd, HHRDDInfo hhrddInfo,
      HHSparkSession hhSparkSession) {
    return new HHDatasetBuilder(hhRdd, hhrddInfo, hhSparkSession);
  }

  public static HHDatasetBuilder createHHDatasetBuilder(HHRDDInfo hhrddInfo,
      HHSparkSession hhSparkSession) {
    return new HHDatasetBuilder(hhrddInfo, hhSparkSession);
  }

  public static HHJavaRDDBuilder createHHJavaRDD(HHRDD hhRdd, HHRDDInfo hhrddInfo,
      HHSparkSession hhSparkSession) {
    return new HHJavaRDDBuilder(hhRdd, hhrddInfo, hhSparkSession);
  }

  public static HHJavaRDDBuilder createHHJavaRDD(HHRDDInfo hhrddInfo,
      HHSparkSession hhSparkSession) {
    return new HHJavaRDDBuilder(hhrddInfo, hhSparkSession);
  }

}
