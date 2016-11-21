/**
 * 
 */
package com.talentica.spark.test;

import java.io.FileNotFoundException;
import java.io.Serializable;

import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDConfig;
import com.talentica.hungryHippos.rdd.HHRDDConfiguration;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.Tuple2;

public class HHRDDTest implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static SparkContext sc;

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("CustomRDDApp");

    try {
      sc = new JavaSparkContext(conf).sc();
      test();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void test() throws FileNotFoundException, JAXBException {
    JobMatrix jobConf = new JobMatrix();
    int count = 0;
    for (int i = 0; i < 1; i++) {
      jobConf.addJob(new Job(new Integer[] {i}, 1, count++));

    }
    System.out.println(count);
    System.out.println(jobConf.toString());
    JavaPairRDD<String, Long> allRDD = null;
    HHRDDConfiguration hhrdConfiguration = new HHRDDConfiguration("sudarshans/100lines",
        "/home/sudarshans/hh/filesystem/sudarshans/100lines/data_",
        "/home/sudarshans/config/client-config.xml");

    HHRDDConfig hhrdConfig =
        new HHRDDConfig(hhrdConfiguration.getRowSize(), hhrdConfiguration.getShardingIndexes(),
            hhrdConfiguration.getDirectoryLocation(), hhrdConfiguration.getShardingFolderPath(),
            hhrdConfiguration.getNodes(), hhrdConfiguration.getDataDescription());

    HHRDD hipposRDD = new HHRDD(sc, hhrdConfig);
    for (Job job : jobConf.getJobs()) {
      JavaPairRDD<String, Long> jvd =
          hipposRDD.toJavaRDD().mapToPair(new PairFunction<HHRDDRowReader, String, Long>() {

            @Override
            public Tuple2<String, Long> call(HHRDDRowReader reader) throws Exception {
              String key = "";
              for (int index = 0; index < job.getDimensions().length; index++) {
                key =
                    key + ((MutableCharArrayString) reader.readAtColumn(job.getDimensions()[index]))
                        .toString();
              }
              key = key + "|id=" + job.getJobId();
              Long value = (Long) reader.readAtColumn(job.getCalculationIndex());
              return new Tuple2<String, Long>(key, value);
            }
          }).reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long x, Long y) {
              return x + y;
            }
          });
      if (allRDD == null) {
        allRDD = jvd;
      } else {
        allRDD = allRDD.union(jvd);
      }
    }
    allRDD.saveAsTextFile("/home/sudarshans/hh/filesystem/output15");
    sc.stop();
  }

}
