/**
 * 
 */
package com.talentica.spark.test;

/**
 * @author pooshans
 *
 */
public class JobRunText {/*

  private static final String MASTER_IP = "spark://pooshans:7077";

  public static void main(String[] args) {
    if (args.length < 3) {
      System.err.println("Invalid args. Correct seq <inputFile> <outputDir> <appName>");
      System.exit(1);
    }
    String inputFile = args[0];
    String outputDir = args[1];
    String appName = args[2];
    List<SumJob> jobList = getJobs();
    SparkSession spark = SparkSession.builder().appName(appName).master(MASTER_IP).getOrCreate();
    JavaRDD<String> input = spark.read().textFile(inputFile).javaRDD();
    JavaPairRDD<String, Double> finalSumRDD = sumJobMapReduce(jobList, input);
    finalSumRDD.saveAsTextFile(outputDir);
  }

  private static JavaPairRDD<String, Double> sumJobMapReduce(List<SumJob> jobList,
      JavaRDD<String> input) {
    JavaPairRDD<String, Double> accumulateSumRDD = null;
    for (SumJob sumJob : jobList) {
      JavaPairRDD<String, Double> sumRDD =
          input.mapToPair(new PairFunction<String, String, Double>() {
            public Tuple2<String, Double> call(String x) {
              String[] token = x.split(",");
              String key = getKey(sumJob.getDimensions(), token);
              Double value = Double.valueOf(token[sumJob.getIndex()]);
              return new Tuple2(key, value);
            }
            private String getKey(int[] dimensions, String[] token) {
              String key = "";
              for (int i = 0; i < dimensions.length; i++) {
                if (i != 0) {
                  key = key + ",";
                }
                key = key + token[dimensions[i]];
              }
              return key;
            }
          }).reduceByKey(new Function2<Double, Double, Double>() {
            public Double call(Double x, Double y) {
              return x + y;
            }
          });
      if (accumulateSumRDD == null) {
        accumulateSumRDD = sumRDD;
      } else {
        accumulateSumRDD = accumulateSumRDD.union(sumRDD);
      }
    }
    return accumulateSumRDD;
  }

  private static List<SumJob> getJobs() {
    List<SumJob> jobList = new ArrayList<>();
    int jobId = 0;

    for (int i = 0; i < 3; i++) {
      jobList.add(new SumJob(new int[] {i}, 6, jobId++));
      jobList.add(new SumJob(new int[] {i}, 7, jobId++));
      for (int j = i + 1; j < 4; j++) {
        jobList.add(new SumJob(new int[] {i, j}, 6, jobId++));
        jobList.add(new SumJob(new int[] {i, j}, 7, jobId++));
        for (int k = j + 1; k < 4; k++) {
          jobList.add(new SumJob(new int[] {i, j, k}, 6, jobId++));
          jobList.add(new SumJob(new int[] {i, j, k}, 7, jobId++));
        }
      }
    }
    return jobList;
  }

*/}
