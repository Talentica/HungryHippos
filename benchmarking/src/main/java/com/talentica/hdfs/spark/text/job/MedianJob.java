/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hdfs.spark.text.job;

import com.talentica.hungryHippos.rdd.main.job.Job;
import com.talentica.hungryHippos.rdd.main.job.JobMatrix;
import com.talentica.hungryhippos.ds.DescriptiveStatisticsNumber;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Iterator;

public class MedianJob {

	private static Logger LOGGER = LoggerFactory.getLogger(MedianJob.class);
	private static JavaSparkContext context;

	public static void main(String[] args){
		String masterIp = args[0];
		String appName = args[1];
		String inputFile = args[2];
		String outputDir = args[3];

		initSparkContext(masterIp,appName);

		JavaRDD<String> rdd = context.textFile(inputFile);
		for(Job job : getSumJobMatrix().getJobs()){
			Broadcast<Job> broadcastJob = context.broadcast(job);
			runJob(rdd,broadcastJob,outputDir);
		}
	}

	private static void initSparkContext(String masterIp,String appName){
		if(context == null){
			SparkConf conf = new SparkConf().setMaster(masterIp).setAppName(appName);
			context = new JavaSparkContext(conf);
		}
	}

	public static void runJob(JavaRDD<String> rdd,Broadcast<Job> broadcastJob,String outputDir){
		JavaPairRDD<String, Double> pairRDD = rdd.mapToPair(new PairFunction<String, String, Double>() {
			private static final long serialVersionUID = -1129787304947692082L;
			@Override
			public Tuple2<String, Double> call(String t) throws Exception {
				String[] line = t.split(",");
				StringBuilder key = new StringBuilder();
				for (int index = 0; index < broadcastJob.value().getDimensions().length; index++) {
					key.append(line[broadcastJob.value().getDimensions()[index]]);
				}
				key.append("|id=").append(broadcastJob.value().getJobId());
				Double value = new Double(line[broadcastJob.value().getCalculationIndex()]);
				return new Tuple2<String, Double>(key.toString(),value);
			}});
		JavaPairRDD<String, Iterable<Double>> pairRDDGroupedByKey = pairRDD.groupByKey();
		JavaPairRDD<String, Double> result = pairRDDGroupedByKey
				.mapToPair(new PairFunction<Tuple2<String, Iterable<Double>>, String, Double>() {
					private static final long serialVersionUID = 484111559311975643L;

					@Override
					public Tuple2<String, Double> call(Tuple2<String, Iterable<Double>> t) throws Exception {
					  DescriptiveStatisticsNumber<Double> medianCalculator = new DescriptiveStatisticsNumber<Double>();
						Iterator<Double> itr = t._2.iterator();
						while (itr.hasNext()) {
							medianCalculator.add(itr.next());
						}
						Double median = (double)medianCalculator.percentile(50);
						return new Tuple2<String, Double>(t._1, median);
					}
				});
		result.saveAsTextFile(outputDir + broadcastJob.value().getJobId());
		LOGGER.info("Output files are in directory {}",
				outputDir +  broadcastJob.value().getJobId());
	}

	private static JobMatrix getSumJobMatrix(){
		JobMatrix medianJobMatrix = new JobMatrix();
		medianJobMatrix.addJob(new Job(new Integer[] {0,1},6,0));
		return medianJobMatrix;
	}
}
