/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDInfo;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.reflect.ClassManifestFactory$;

/**
 * To create the data set for given HungryHippos RDD.
 * 
 * @author pooshans
 * @param <T>
 *
 */
public class HHDataset implements Serializable {
  private static final long serialVersionUID = 3564747948683195956L;
  private JavaRDD<byte[]> javaRdd;
  private SparkSession sparkSession;
  private Broadcast<HHRDDRowReader> hhBroadcasetReader;

  /**
   * Constructor of HHDataset
   * 
   * @param hhRdd
   * @param hhrddInfo
   * @param sparkSession
   */
  public HHDataset(HHRDD hhRdd, HHRDDInfo hhrddInfo, SparkSession sparkSession) {
    this.javaRdd = hhRdd.toJavaRDD();
    this.sparkSession = sparkSession;
    hhBroadcasetReader =
        sparkSession.sparkContext().broadcast(new HHRDDRowReader(hhrddInfo.getFieldDataDesc()),
            ClassManifestFactory$.MODULE$.fromClass(HHRDDRowReader.class));
  }

  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row.
   * 
   * @param beanClazz
   * @return Dataset<Row>
   * @throws ClassNotFoundException
   */
  public <T> Dataset<Row> toDatasetByRow(Class<T> beanClazz) throws ClassNotFoundException {
    JavaRDD<T> rddDataframe = javaRdd.map(new Function<byte[], T>() {
      @Override
      public T call(byte[] b) throws Exception {
        HHRDDRowReader hhrddRowReader = hhBroadcasetReader.getValue();
        hhrddRowReader.setByteBuffer(ByteBuffer.wrap(b));
        return getTuple(beanClazz, hhrddRowReader);
      }
    });
    Dataset<Row> dataset =
        sparkSession.sqlContext().createDataFrame(rddDataframe, Class.forName(beanClazz.getName()));
    return dataset;
  }

  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row for each partition.
   * 
   * @param beanClazz
   * @return
   * @throws ClassNotFoundException
   */
  public <T> Dataset<Row> toDatasetByPartition(Class<T> beanClazz) throws ClassNotFoundException {
    JavaRDD<T> rddDataframe = javaRdd.mapPartitions(new FlatMapFunction<Iterator<byte[]>, T>() {
      @Override
      public Iterator<T> call(Iterator<byte[]> t) throws Exception {
        List<T> tupleList = new ArrayList<T>();
        HHRDDRowReader hhrddRowReader = hhBroadcasetReader.getValue();
        while (t.hasNext()) {
          hhrddRowReader.setByteBuffer(ByteBuffer.wrap(t.next()));
          tupleList.add(getTuple(beanClazz, hhrddRowReader));
        }
        return tupleList.iterator();
      }
    }, true);
    Dataset<Row> dataset =
        sparkSession.sqlContext().createDataFrame(rddDataframe, Class.forName(beanClazz.getName()));
    return dataset;
  }

  private <T> T getTuple(Class<T> beanClazz, HHRDDRowReader hhrddRowReader)
      throws NoSuchFieldException, IllegalAccessException, InstantiationException {
    return new HHTupleType<T>(hhrddRowReader) {
      @Override
      public T createTuple() throws InstantiationException, IllegalAccessException {
        return (T) beanClazz.newInstance();
      }
    }.getTuple();
  }
}
