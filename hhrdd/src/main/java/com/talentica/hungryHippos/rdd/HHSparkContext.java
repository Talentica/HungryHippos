package com.talentica.hungryHippos.rdd;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.rdd.job.Job;

public class HHSparkContext extends JavaSparkContext {

  private Map<String, HHRDDInfo> hhrddInfoCache = new HashMap<>();

  public HHSparkContext(SparkConf config, String clientConfigurationFilePath)
      throws FileNotFoundException, JAXBException {
    super(config);
    HHRDDHelper.initialize(clientConfigurationFilePath);
  }

  public JavaRDD<byte[]> binaryRecords(Job job, String hhFilePath)
      throws FileNotFoundException, JAXBException {
    HHRDDInfo hhrddInfo = hhrddInfoCache.get(hhFilePath);
    if (hhrddInfo == null) {
      hhrddInfo = HHRDDHelper.getHhrddInfo(hhFilePath);
      hhrddInfoCache.put(hhFilePath, hhrddInfo);
    }
    return new HHRDD(this, hhrddInfo, job.getDimensions(), true).toJavaRDD();
  }

  public Broadcast<DataDescription> broadcastFieldDataDescription(String hhFilePath) {
    return broadcast(hhrddInfoCache.get(hhFilePath).getFieldDataDesc());
  }

  public int[] getShardingIndexes(String hhFilePath) {
    return hhrddInfoCache.get(hhFilePath).getShardingIndexes();
  }

  public String getActualPath(String path) {
    return HHRDDHelper.getActualPath(path);
  }


}
