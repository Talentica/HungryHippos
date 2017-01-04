/**
 * 
 */
package com.talentica.hungryHippos.rdd.utility;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.util.Iterator;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * @author pooshans
 *
 */
public class HHRDDFileUtils implements Serializable {

  private static final long serialVersionUID = 4888239993511591404L;

  public static <T> void saveAsText(JavaRDD<T> javaRDD, String path) {
    javaRDD.foreachPartition(new VoidFunction<Iterator<T>>() {

      @Override
      public void call(Iterator<T> t) throws Exception {
        int partitionId = TaskContext.getPartitionId();
        new File(path).mkdirs();
        String filePath = path + File.separatorChar + "part-" + partitionId;
        File file = new File(filePath);
        BufferedWriter out = new BufferedWriter(new FileWriter(file), 20480);
        Tuple2<?, ?> tuple2 = null;
        while (t.hasNext()) {
          T token = t.next();
          if (token instanceof Tuple2<?, ?>) {
            tuple2 = (Tuple2<?, ?>) token;
          }else{
            return;
          }
          out.write(tuple2._1 + "," + tuple2._2);
          out.newLine();
        }
        out.flush();;
        out.close();
      }
    });
  }

}
