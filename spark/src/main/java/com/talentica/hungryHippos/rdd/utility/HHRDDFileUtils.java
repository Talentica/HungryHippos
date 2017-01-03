/**
 * 
 */
package com.talentica.hungryHippos.rdd.utility;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Iterator;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * @author pooshans
 *
 */
public class HHRDDFileUtils{

	
	 public static <T> void saveAsText(JavaRDD<T> javaRDD, String path) throws FileNotFoundException {
		 
			javaRDD.foreachPartition(new VoidFunction<Iterator<T>>() {
				private static final long serialVersionUID = 3063234774247862077L;
				int partitionId = TaskContext.getPartitionId();
				File file = new File(path + File.separatorChar + "part-" + partitionId);
				FileOutputStream fos = new FileOutputStream(file, true);
				PrintStream out = new PrintStream(fos);

				@Override
				public void call(Iterator<T> t) throws Exception {
					Tuple2<?, ?>	tuple2 = null;
					while (t.hasNext()) {
						T token = t.next();
						if(token instanceof Tuple2<?, ?>){
							tuple2 = (Tuple2<?, ?>) token;
						}
						out.println(tuple2._1 + "|" + tuple2._2);
					}
					out.close();
					fos.close();
				}
			});
		}
	 
}
