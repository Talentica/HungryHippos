/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.File;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;

import com.talentica.hungryHippos.rdd.utility.HHRDDFileUtils;

import scala.reflect.ClassTag;

/**
 * @author pooshans
 *
 */
public class HHJavaRDD<T> extends JavaRDD<T>{

	private static final long serialVersionUID = -8143851746121063874L;
	
	public HHJavaRDD(RDD<T> rdd, ClassTag<T> classTag) {
		super(rdd, classTag);
	}
	
	@Override
	  public void saveAsTextFile(String path){
		
			HHRDDFileUtils.saveAsText(this, path);
	  }

}
