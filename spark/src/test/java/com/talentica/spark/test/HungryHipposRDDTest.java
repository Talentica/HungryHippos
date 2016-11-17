/**
 * 
 */
package com.talentica.spark.test;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.HungryHipposRDD;
import com.talentica.hungryHippos.rdd.HungryHipposRDDConf;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobConf;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.Tuple2;

public class HungryHipposRDDTest implements Serializable {

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

  public static void test() {
    JobConf jobConf = new JobConf();
    jobConf.addJob(new Job(new Integer[] {0}, 6));
    jobConf.addJob(new Job(new Integer[] {0}, 7));
    jobConf.addJob(new Job(new Integer[] {0, 1}, 6));
    jobConf.addJob(new Job(new Integer[] {0, 1}, 7));

    JavaPairRDD<String, Double> allRDD = null;

    HungryHipposRDD hipposRDD = new HungryHipposRDD(sc,
        new HungryHipposRDDConf(
            "/home/pooshans/HungryHippos/HungryHippos/configuration-schema/src/main/resources/distribution",
            "/home/pooshans/hhuser/hh/filesystem/distr/data/data_"));
    for (Job job : jobConf.getJobs()) {
      JavaPairRDD<String, Double> jvd =
          hipposRDD.toJavaRDD().mapToPair(new PairFunction<HHRDDRowReader, String, Double>() {

            @Override
            public Tuple2<String, Double> call(HHRDDRowReader reader) throws Exception {
              String key = "";
              for (int index = 0; index < job.getDimensions().length; index++) {
                key =
                    key + ((MutableCharArrayString) reader.readAtColumn(job.getDimensions()[index]))
                        .toString();
              }
              Double value = (Double) reader.readAtColumn(job.getCalculationIndex());
              return new Tuple2<String, Double>(key, value);
            }
          }).reduceByKey(new Function2<Double, Double, Double>() {
            public Double call(Double x, Double y) {
              return x + y;
            }
          });
      if (allRDD == null) {
        allRDD = jvd;
      } else {
        allRDD = allRDD.union(jvd);
      }
    }
    allRDD.saveAsTextFile("/home/pooshans/hhuser/hh/filesystem/distr/data/output4");
    sc.stop();
  }

}
