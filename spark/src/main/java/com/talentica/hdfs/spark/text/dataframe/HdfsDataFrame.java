/**
 * 
 */
package com.talentica.hdfs.spark.text.dataframe;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.talentica.hdfs.spark.binary.job.DataDescriptionConfig;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.rdd.job.Job;
import com.talentica.hungryHippos.rdd.job.JobMatrix;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

/**
 * @author pooshans
 *
 */
public class HdfsDataFrame {

  private static JavaSparkContext context;

  public static void main(String[] args)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    String masterIp = args[0];
    String appName = args[1];
    String inputFile = args[2];
    String outputDir = args[3];
    String shardingFolderPath = args[4];

    initSparkContext(masterIp, appName);
    DataDescriptionConfig dataDescriptionConfig = new DataDescriptionConfig(shardingFolderPath);
    JavaRDD<byte[]> rdd = context.binaryRecords(inputFile, dataDescriptionConfig.getRowSize());
    FieldTypeArrayDataDescription fieldTypeArrayDataDescription =
        dataDescriptionConfig.getDataDescription();
    int columns = dataDescriptionConfig.getShardingClientConf().getInput().getDataDescription()
        .getColumn().size();
    HHRDDRowReader hhrddRowReader = new HHRDDRowReader(dataDescriptionConfig.getDataDescription());
    JavaRDD<SchemaBean> rdd1 =
        rdd.mapPartitions(new FlatMapFunction<Iterator<byte[]>, SchemaBean>() {
          @Override
          public Iterator<SchemaBean> call(Iterator<byte[]> t) throws Exception {
            List<SchemaBean> readerList = new ArrayList<SchemaBean>();
            while (t.hasNext()) {
              byte[] b = t.next();
              hhrddRowReader.setByteBuffer(ByteBuffer.wrap(b));
              SchemaBuilder schemaBuilder = new SchemaBuilder(hhrddRowReader, columns);
              readerList.add(schemaBuilder.getSchemaBean());
            }
            return readerList.iterator();
          }
        }, true);
    SparkSession sparkSession =
        SparkSession.builder().master("local[*]").appName("test").getOrCreate();
    Dataset<Row> rows = sparkSession.sqlContext().createDataFrame(rdd1, SchemaBean.class);
    rows.createOrReplaceTempView("data");
    Dataset<Row> rs = sparkSession.sql("SELECT * FROM data WHERE column1 like 'h' ");
    rs.show();
    context.stop();
  }

  private static void initSparkContext(String masterIp, String appName) {
    if (context == null) {
      SparkConf conf = new SparkConf().setMaster(masterIp).setAppName(appName);
      context = new JavaSparkContext(conf);
    }
  }

  private static JobMatrix getSumJobMatrix()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    JobMatrix medianJobMatrix = new JobMatrix();
    medianJobMatrix.addJob(new Job(new Integer[] {0, 1}, 6, 0));
    return medianJobMatrix;
  }
}
