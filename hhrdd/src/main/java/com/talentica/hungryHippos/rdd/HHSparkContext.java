package com.talentica.hungryHippos.rdd;

import com.talentica.hungryHippos.client.domain.DataDescription;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

public class HHSparkContext extends JavaSparkContext {

  private Map<String, HHRDDInfo> hhrddInfoCache = new HashMap<>();

  public HHSparkContext(SparkConf config, String clientConfigurationFilePath)
      throws FileNotFoundException, JAXBException {
    super(config);
    HHRDDHelper.initialize(clientConfigurationFilePath);
  }

  public JavaRDD<byte[]> binaryRecords(Integer[] jobDimensions, String hhFilePath, boolean requiresShuffle)
      throws FileNotFoundException, JAXBException {
    HHRDDInfo hhrddInfo = getHHRDDInfo(hhFilePath);
    return new HHRDD(this, hhrddInfo, jobDimensions, requiresShuffle).toJavaRDD();
  }

  private HHRDDInfo getHHRDDInfo(String hhFilePath)
          throws JAXBException, FileNotFoundException {
    HHRDDInfo hhrddInfo = hhrddInfoCache.get(hhFilePath);
    if (hhrddInfo == null) {
      hhrddInfo = HHRDDHelper.getHhrddInfo(hhFilePath);
      hhrddInfoCache.put(hhFilePath, hhrddInfo);
    }
    return hhrddInfo;
  }

  public Broadcast<DataDescription> broadcastFieldDataDescription(String hhFilePath)
          throws JAXBException, FileNotFoundException {
    return broadcast(getHHRDDInfo(hhFilePath).getFieldDataDesc());
  }

  public int[] getShardingIndexes(String hhFilePath)
          throws JAXBException, FileNotFoundException {
    return getHHRDDInfo(hhFilePath).getShardingIndexes();
  }

  public String getActualPath(String path) {
    return HHRDDHelper.getActualPath(path);
  }


}
