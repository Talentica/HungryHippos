/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import org.apache.spark.sql.SparkSession;

import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDInfo;

/**
 * Factory used to construct the object for data frame.
 * 
 * @author pooshans
 *
 */
public class HHDataframeFactory {

  public static HHDatasetBuilder createHHDataset(HHRDD hhRdd, HHRDDInfo hhrddInfo,
      SparkSession sparkSession) {
    return new HHDatasetBuilder(hhRdd, hhrddInfo, sparkSession);
  }

  public static HHDatasetBuilder createHHDatasetBuilder(HHRDDInfo hhrddInfo) {
    return new HHDatasetBuilder(hhrddInfo);
  }

  public static HHJavaRDDBuilder createHHJavaRDD(HHRDD hhRdd, HHRDDInfo hhrddInfo,
      SparkSession sparkSession) {
    return new HHJavaRDDBuilder(hhRdd, hhrddInfo, sparkSession);
  }

  public static HHJavaRDDBuilder createHHJavaRDD(HHRDDInfo hhrddInfo) {
    return new HHJavaRDDBuilder(hhrddInfo);
  }

}
