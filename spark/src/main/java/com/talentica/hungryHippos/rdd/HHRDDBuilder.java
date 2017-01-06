/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;

import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;

/**
 * @author pooshans
 *
 */
public class HHRDDBuilder {

  private static Map<String, HHRDD> cacheRDD = new HashMap<>();
  private JavaSparkContext context;
  private HHRDDConfigSerialized hhrddConfigSerialized;
  
  public HHRDDBuilder(JavaSparkContext context,HHRDDConfigSerialized hhrddConfigSerialized){
    this.context = context;
    this.hhrddConfigSerialized =hhrddConfigSerialized;
  }
  
  
  public HHRDD gerOrCreateRDD(Job job) {
    if (hhrddConfigSerialized == null) {
      throw new RuntimeException("Please initialize the HHRDDBuilder");
    }
    String keyOfHHRDD =
        HHRDDHelper.generateKeyForHHRDD(job, hhrddConfigSerialized.getShardingIndexes());
    HHRDD hipposRDD = cacheRDD.get(keyOfHHRDD);
    if (hipposRDD == null) {
      hipposRDD = new HHRDD(context, hhrddConfigSerialized, job.getDimensions());
      cacheRDD.put(keyOfHHRDD, hipposRDD);
    }
    return hipposRDD;

  }

}
