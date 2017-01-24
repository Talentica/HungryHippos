/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.activation.UnsupportedDataTypeException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDInfo;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

import scala.reflect.ClassManifestFactory;

/**
 * To create the data set for given HungryHippos RDD.
 * 
 * @author pooshans
 * @param <T>
 *
 */
public class HHDatasetConverter implements Serializable {
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
  public HHDatasetConverter(HHRDD hhRdd, HHRDDInfo hhrddInfo, SparkSession sparkSession) {
    this.javaRdd = hhRdd.toJavaRDD();
    this.sparkSession = sparkSession;
    hhBroadcasetReader =
        sparkSession.sparkContext().broadcast(new HHRDDRowReader(hhrddInfo.getFieldDataDesc()),
            ClassManifestFactory.fromClass(HHRDDRowReader.class));
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

  public Dataset<Row> toDatasetStructType(String[] fieldName) throws UnsupportedDataTypeException {
    List<StructField> fields = new ArrayList<>();
    HHRDDRowReader hhrddRowReader = hhBroadcasetReader.getValue();
    int columns = hhrddRowReader.getFieldDataDescription().getNumberOfDataFields();
    for (int index = 0; index < columns; index++) {
      DataLocator locator = hhrddRowReader.getFieldDataDescription().locateField(index);
      StructField field = null;
      switch (locator.getDataType()) {
        case BYTE:
          field = DataTypes.createStructField(fieldName[index], DataTypes.ByteType, true);
          break;
        case CHAR:
          field = DataTypes.createStructField(fieldName[index], DataTypes.StringType, true);
          break;
        case SHORT:
          field = DataTypes.createStructField(fieldName[index], DataTypes.ShortType, true);
          break;
        case INT:
          field = DataTypes.createStructField(fieldName[index], DataTypes.IntegerType, true);
          break;
        case LONG:
          field = DataTypes.createStructField(fieldName[index], DataTypes.LongType, true);
          break;
        case FLOAT:
          field = DataTypes.createStructField(fieldName[index], DataTypes.FloatType, true);
          break;
        case DOUBLE:
          field = DataTypes.createStructField(fieldName[index], DataTypes.DoubleType, true);
          break;
        case STRING:
          field = DataTypes.createStructField(fieldName[index], DataTypes.StringType, true);
          break;
        default:
          throw new UnsupportedDataTypeException("Invalid field data type");
      }
      fields.add(field);
    }
    StructType schema = DataTypes.createStructType(fields);
    JavaRDD<Row> rowRDD = javaRdd.map(new Function<byte[], Row>() {
      @Override
      public Row call(byte[] b) throws Exception {
        hhrddRowReader.setByteBuffer(ByteBuffer.wrap(b));
        Object[] tuple = new Object[columns];
        for (int index = 0; index < columns; index++) {
          Object obj = hhrddRowReader.readAtColumn(index);
          if (obj instanceof MutableCharArrayString) {
            tuple[index] = ((MutableCharArrayString) hhrddRowReader.readAtColumn(index)).toString();
          } else if (obj instanceof Double) {
            tuple[index] = ((Double) (hhrddRowReader.readAtColumn(index)));
          } else if (obj instanceof Integer) {
            tuple[index] = ((Integer) (hhrddRowReader.readAtColumn(index)));
          } else if (obj instanceof Byte) {
            tuple[index] = ((Byte) (hhrddRowReader.readAtColumn(index)));
          } else if (obj instanceof Character) {
            tuple[index] = ((Character) (hhrddRowReader.readAtColumn(index)));
          } else if (obj instanceof Short) {
            tuple[index] = ((Short) (hhrddRowReader.readAtColumn(index)));
          } else if (obj instanceof Long) {
            tuple[index] = ((Long) (hhrddRowReader.readAtColumn(index)));
          } else if (obj instanceof Float) {
            tuple[index] = ((Float) (hhrddRowReader.readAtColumn(index)));
          } else {
            throw new UnsupportedDataTypeException("Invalid data type conversion");
          }
        }
        return RowFactory.create(tuple);
      }
    });
    return sparkSession.sqlContext().createDataFrame(rowRDD, schema);
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
