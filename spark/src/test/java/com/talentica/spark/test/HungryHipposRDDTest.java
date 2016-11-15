/**
 * 
 */
package com.talentica.spark.test;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.rdd.HungryHipposRDD;
import com.talentica.hungryHippos.rdd.HungryHipposRDDConf;

public class HungryHipposRDDTest {
  
  private SparkContext sc;
  
  @Before
  public void setUp(){
    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("CustomRDDApp");
    try (JavaSparkContext sc = new JavaSparkContext(conf)) {
      this.sc = sc.sc();
    }
  }
  
  @Test
  public void test(){
    HungryHipposRDD hipposRDD = new HungryHipposRDD(sc, new HungryHipposRDDConf(50, 33, new int[] {0,1,2}));
    }
}
