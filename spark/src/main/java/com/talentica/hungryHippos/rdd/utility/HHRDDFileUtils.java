/**
 * 
 */
package com.talentica.hungryHippos.rdd.utility;

import java.io.File;
import java.io.IOException;
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
		try {
			javaRDD.foreachPartition(new VoidFunction<Iterator<T>>() {
				private static final long serialVersionUID = 3063234774247862077L;
				int partitionId = TaskContext.getPartitionId();
				boolean flag = new File(path + File.separatorChar + "part-" + partitionId).createNewFile();
				File file = new File(path + File.separatorChar + "part-" + partitionId);
			   SerializablePrintStream out = new SerializablePrintStream(file);

				@Override
				public void call(Iterator<T> t) throws Exception {
					System.out.println("File " + file.getAbsolutePath() + " status " + flag );
					if(!flag) return;
					Tuple2<?, ?> tuple2 = null;
					while (t.hasNext()) {
						T token = t.next();
						if (token instanceof Tuple2<?, ?>) {
							tuple2 = (Tuple2<?, ?>) token;
						}
						out.println(tuple2._1 + "|" + tuple2._2);
					}
					out.getPrintStream().flush();;
					out.close();
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
