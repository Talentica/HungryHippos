/**
 * 
 */
package com.talentica.spark.job.executor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.HHJavaRDD;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDConfigSerialized;
import com.talentica.hungryHippos.rdd.HHRDDConfiguration;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.Tuple2;

/**
 * @author pooshans
 *
 */
public class SumJobExecutor implements Serializable {

	private static final long serialVersionUID = 4405057963355945497L;
	private static Logger LOGGER = LoggerFactory.getLogger(SumJobExecutor.class);
	private String outputFile;
	private String distrDir;
	private String clientConf;
	private String masterIp;
	private String appName;
	private Map<Integer, HHRDD> cahceRDD;

	/**
	 * @param args
	 */
	public SumJobExecutor(String args[]) {
		validateProgramArgument(args);
		this.masterIp = args[0];
		this.appName = args[1];
		this.distrDir = args[2];
		this.clientConf = args[3];
		this.outputFile = args[4];
		this.cahceRDD = new HashMap<Integer, HHRDD>();
	}

	@SuppressWarnings("serial")
	public void startSumJob(JavaSparkContext context, HHRDDConfiguration hhrddConfiguration,
			Broadcast<Job> jobBroadcast, int jobPrimDim) throws FileNotFoundException, JAXBException {
		HHRDDConfigSerialized hhrddConfigSerialized = new HHRDDConfigSerialized(hhrddConfiguration.getRowSize(),
				hhrddConfiguration.getShardingKeyOrder(), hhrddConfiguration.getDirectoryLocation(),
				hhrddConfiguration.getShardingFolderPath(), hhrddConfiguration.getNodes(),
				hhrddConfiguration.getDataDescription(), jobPrimDim);

		Broadcast<FieldTypeArrayDataDescription> dataDes = context.broadcast(hhrddConfiguration.getDataDescription());

		HHRDD hipposRDD = cahceRDD.get(jobPrimDim);

		if (hipposRDD == null) {
			hipposRDD = new HHRDD(context, hhrddConfigSerialized);
			cahceRDD.put(jobPrimDim, hipposRDD);
		}
		JavaPairRDD<String, Integer> javaPairRDD = hipposRDD.toJavaRDD()
				.mapToPair(new PairFunction<byte[], String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(byte[] bytes) throws Exception {
						HHRDDRowReader reader = new HHRDDRowReader(dataDes.getValue());
						ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
						reader.setByteBuffer(byteBuffer);
						String key = "";
						for (int index = 0; index < jobBroadcast.value().getDimensions().length; index++) {
							key = key + ((MutableCharArrayString) reader
									.readAtColumn(jobBroadcast.value().getDimensions()[index])).toString();
						}
						key = key + "|id=" + jobBroadcast.value().getJobId();
						Integer value = (Integer) reader.readAtColumn(jobBroadcast.value().getCalculationIndex());
						return new Tuple2<String, Integer>(key, value);
					}
				});
		JavaRDD<Tuple2<String, Long>> JavaRDD =  javaPairRDD
				.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, Tuple2<String, Long>>() {

					@Override
					public Iterator<Tuple2<String, Long>> call(Iterator<Tuple2<String, Integer>> t) throws Exception {

						List<Tuple2<String, Long>> sumList = new ArrayList<>();
						Map<String, Long> map = new HashMap<>();
						while (t.hasNext()) {
							Tuple2<String, Integer> tuple2 = t.next();
							Long storedValue = map.get(tuple2._1);
							if (storedValue == null) {
								map.put(tuple2._1, Long.valueOf(tuple2._2.intValue()));
							} else {
								map.put(tuple2._1, tuple2._2 + storedValue);
							}
						}

						for (Map.Entry<String, Long> entry : map.entrySet()) {
							sumList.add(new Tuple2<String, Long>(entry.getKey(), entry.getValue()));
						}
						return sumList.iterator();
					}
				}, true);
		
		HHJavaRDD<Tuple2<String, Long>> hhJavaRDD = new HHJavaRDD<Tuple2<String, Long>>(JavaRDD.rdd(), JavaRDD.classTag());
		
		hhJavaRDD.saveAsTextFile(hhrddConfiguration.getOutputFile() +File.separatorChar + jobBroadcast.value().getJobId());
		LOGGER.info("Output files are in directory {}",
				hhrddConfiguration.getOutputFile() + jobBroadcast.value().getJobId());

	}

	public void stop(JavaSparkContext context) {
		context.stop();
	}

	public String getOutputFile() {
		return outputFile;
	}

	public void setOutputFile(String outputFile) {
		this.outputFile = outputFile;
	}

	public String getDistrDir() {
		return distrDir;
	}

	public void setDistrDir(String distrDir) {
		this.distrDir = distrDir;
	}

	public String getClientConf() {
		return clientConf;
	}

	public void setClientConf(String clientConf) {
		this.clientConf = clientConf;
	}

	public String getMasterIp() {
		return masterIp;
	}

	public void setMasterIp(String masterIp) {
		this.masterIp = masterIp;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	private void validateProgramArgument(String args[]) {
		if (args.length < 5) {
			System.err.println(
					"Improper arguments. Please provide in  proper order. i.e <spark-master-ip> <application-name> <distributed-directory> <client-configuration> <ouput-file-name>");
			System.out.println(
					"Parameter argumes should be {spark://{master}:7077} {test-app} {/distr/data} {{client-path}/client-config.xml} {output}");
			System.exit(1);
		}
	}

}
