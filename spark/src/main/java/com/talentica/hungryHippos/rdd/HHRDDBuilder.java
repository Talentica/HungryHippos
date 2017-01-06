/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.spark.api.java.JavaSparkContext;

import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.utility.HHRDDHelper;

/**
 * @author pooshans
 *
 */
public class RDDBuilder {

  private static Map<String, HHRDD> cacheRDD = new HashMap<>();
  private static HHRDDConfigSerialized hhrddConfigSerialized;

  public static void initialize(String distrDir)
      throws FileNotFoundException, JAXBException {
    hhrddConfigSerialized = HHRDDHelper.getHhrddConfigSerialized(distrDir);
  }

  public static HHRDD gerOrCreateRDD(Job job, JavaSparkContext context) {
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
