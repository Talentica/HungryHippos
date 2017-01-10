//package com.talentica.hungryhippos;
//
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.PrintWriter;
//import java.io.UnsupportedEncodingException;
//import java.nio.ByteBuffer;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.ArrayList;
//import java.util.List;
//
//import javax.xml.bind.JAXBException;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//
//import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
//import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
//import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
//import com.talentica.hungryhippos.config.sharding.Column;
//import com.talentica.hungryhippos.config.sharding.ShardingClientConfig;
//
//import scala.Tuple2;
//
//public class SparkAppSharded {
//
//  public static void main(String[] args)
//      throws FileNotFoundException, UnsupportedEncodingException, JAXBException {
//
//    String shardingClientConfigLoc = "/home/sudarshans/config/sharding-client-config.xml";
//    String master = "spark://sudarshans:7077";
//
//    SparkConf conf = new SparkConf().setAppName("test").setMaster(master);
//    JavaSparkContext sc = new JavaSparkContext(conf);
//    ShardingClientConfig shardedConfig =
//        JaxbUtil.unmarshalFromFile(shardingClientConfigLoc, ShardingClientConfig.class);
//    List<Column> columns = shardedConfig.getInput().getDataDescription().getColumn();
//    String[] dataTypeDescription = new String[columns.size()];
//
//    for (int index = 0; index < columns.size(); index++) {
//      String element = columns.get(index).getDataType() + "-" + columns.get(index).getSize();
//      dataTypeDescription[index] = element;
//    }
//    final FieldTypeArrayDataDescription dataDescription =
//        FieldTypeArrayDataDescription.createDataDescription(dataTypeDescription,
//            shardedConfig.getMaximumSizeOfSingleBlockData());
//
//
//    // List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
//    /*
//     * JavaRDD<Integer> rd = sc.parallelize(data); // System.out.println(rd.reduce((a,b) -> (a+b)));
//     * Integer value = rd.reduce(new Function2<Integer, Integer, Integer>() {
//     * 
//     * @Override public Integer call(Integer v1, Integer v2) throws Exception { // TODO
//     * Auto-generated method stub return v1 + v2; } }); System.out.println(value);
//     */
//    // JavaRDD<String> rd = sc.textFile("/home/sudarshans/data_spark_char.csv");
//
//
//    String parentFolder = "/home/sudarshans/hh/filesystem/sudarshans/100lines/data_1";
//
//    List<String> leafFolder = getAllRgularFilesPath(parentFolder);
//
//    JavaRDD<byte[]> rd = null;
//
//    for (String leaf : leafFolder) {
//      JavaRDD<byte[]> rd_temp = sc.binaryRecords(leaf, dataDescription.getSize());
//      if (rd == null) {
//        rd = rd_temp;
//      } else {
//        rd = rd.union(rd_temp);
//      }
//    }
//
//
//    StringBuilder sb = new StringBuilder();
//    JavaRDD<String> jvd = rd.map(new Function<byte[], String>() {
//      /**
//       * 
//       */
//      private static final long serialVersionUID = 1L;
//      private DynamicMarshal dm = new DynamicMarshal(dataDescription);
//
//      @Override
//      public String call(byte[] v1) throws Exception {
//        ByteBuffer byteBuff = ByteBuffer.wrap(v1);
//        sb.delete(0, sb.length());
//        for (int i = 0; i < dataDescription.getNumberOfDataFields(); i++) {
//
//          Object readableData = dm.readValue(i, byteBuff);
//          if (i != 0 && i != dataDescription.getNumberOfDataFields()) {
//            sb.append(",");
//          }
//          sb.append(String.valueOf(readableData));
//
//        }
//        System.out.println(" value : " + sb.toString());
//        return sb.toString();
//      }
//    });
//
//    JavaPairRDD<String, Long> jvpd = jvd.mapToPair(new PairFunction<String, String, Long>() {
//
//      /**
//       * 
//       */
//      private static final long serialVersionUID = 1L;
//
//      @Override
//      public Tuple2<String, Long> call(String t) throws Exception {
//        System.out.println("t is :" + t);
//        String[] s = t.split(",");
//        return new Tuple2<>(s[0], Long.parseLong(s[1]));
//      }
//    });
//
//    JavaPairRDD<String, Long> sum = jvpd.reduceByKey(new Function2<Long, Long, Long>() {
//
//      /**
//      * 
//      */
//      private static final long serialVersionUID = 1L;
//
//      @Override
//      public Long call(Long v1, Long v2) throws Exception {
//
//        return v1 + v2;
//      }
//    });
//
//    List<Tuple2<String, Long>> l = sum.collect();
//    PrintWriter pw = new PrintWriter("/home/sudarshans/spark_sum_byte1.txt", "UTF-8");
//    for (Tuple2<String, Long> tuple2 : l) {
//      String s1 = tuple2._1 + " " + tuple2._2;
//      pw.println(s1);
//      System.out.println(s1);
//    }
//    pw.close();
//    sc.close();
//  }
//
//
//
//  public static List<String> getAllRgularFilesPath(String loc) {
//
//    List<String> filesPresentInRootFolder = new ArrayList<>();
//    try {
//      Files.walk(Paths.get(loc)).forEach(filePath -> {
//        if (Files.isRegularFile(filePath)) {
//          filesPresentInRootFolder.add(filePath.getParent().toString());
//        }
//      });
//    } catch (IOException e) {
//
//    }
//    return filesPresentInRootFolder;
//  }
//
//
//
//}
