/**
 * 
 */
package com.talentica.spark.test;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.HungryHipposRDD;
import com.talentica.hungryHippos.rdd.HungryHipposRDDConf;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.Tuple2;

public class HungryHipposRDDTest implements Serializable {

  private static final long serialVersionUID = -4215844318474916378L;
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
    HungryHipposRDD hipposRDD = new HungryHipposRDD(sc, new HungryHipposRDDConf(2,
        "/home/sudarshans/config", "/home/sudarshans/hh/filesystem/sudarshans/100lines/test"));
    JavaPairRDD<MutableCharArrayString, Long> jvd = hipposRDD.toJavaRDD()
        .mapToPair(new PairFunction<HHRDDRowReader, MutableCharArrayString, Long>() {

          @Override
          public Tuple2<MutableCharArrayString, Long> call(HHRDDRowReader reader)
              throws Exception {
            MutableCharArrayString key = (MutableCharArrayString) reader.readAtColumn(0);
            Long value = (Long) reader.readAtColumn(1);
            return new Tuple2<MutableCharArrayString, Long>(key, value);
          }

        });

    System.out.println();
    jvd.foreach(new VoidFunction<Tuple2<MutableCharArrayString, Long>>() {

      @Override
      public void call(Tuple2<MutableCharArrayString, Long> t) throws Exception {

        System.out.println(t._1 + " " + t._2);

      }
    });
    sc.stop();
  }
}
