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

/**
 * To build the data set for given HungryHippos RDD which convert the underlying HH data
 * representation to the Spark recognized data format.
 * 
 * @author pooshans
 * @param <T>
 *
 */
public class HHDatasetBuilder implements Serializable {
  private static final long serialVersionUID = 3564747948683195956L;
  private JavaRDD<byte[]> javaRdd;
  private SparkSession sparkSession;
  private HHRDDRowReader hhRDDReader;

  /**
   * Constructor of {@code HHDatasetConverter} which is instantiated if client requires prebuild
   * {@code Dataset}.
   * 
   * @param hhRdd
   * @param hhrddInfo
   * @param sparkSession
   */
  public HHDatasetBuilder(HHRDD hhRdd, HHRDDInfo hhrddInfo, SparkSession sparkSession) {
    this.javaRdd = hhRdd.toJavaRDD();
    this.sparkSession = sparkSession;
    hhRDDReader = new HHRDDRowReader(hhrddInfo.getFieldDataDesc());
  }

  /**
   * Constructor of {@code HHDatasetConverter} which is required to only if clien need utility such
   * as {@link #getRow(byte[])} or {@link #createSchema(String[])}
   * 
   * @param hhrddInfo
   */
  public HHDatasetBuilder(HHRDDInfo hhrddInfo) {
    hhRDDReader = new HHRDDRowReader(hhrddInfo.getFieldDataDesc());
  }

  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row.
   * 
   * @param beanClazz
   * @return {@code Dataset<Row>}
   * @throws ClassNotFoundException
   */
  public <T> Dataset<Row> mapToBeanDS(Class<T> beanClazz) throws ClassNotFoundException {
    JavaRDD<T> rddDataframe = javaRdd.map(new Function<byte[], T>() {
      @Override
      public T call(byte[] b) throws Exception {
        hhRDDReader.setByteBuffer(ByteBuffer.wrap(b));
        return getTuple(beanClazz);
      }
    });
    Dataset<Row> dataset =
        sparkSession.sqlContext().createDataFrame(rddDataframe, Class.forName(beanClazz.getName()));
    return dataset;
  }

  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row. It is supported for schema definition of {@code StructType}.
   * 
   * @param fieldName : It is user defined column name. i.e new String[] {"Col1", "Col2", "Col3",
   *        "Col4", "Col5", "Col6", "Col7", "Col8", "Col9"}
   * @return {@code Dataset<Row>}
   * @throws ClassNotFoundException
   * @throws UnsupportedDataTypeException
   */
  public Dataset<Row> mapToStructTypeDS(String[] fieldName)
      throws ClassNotFoundException, UnsupportedDataTypeException {
    StructType schema = createSchema(fieldName);
    JavaRDD<Row> rowRDD = javaRdd.map(new Function<byte[], Row>() {
      @Override
      public Row call(byte[] b) throws Exception {
        return getRow(b);
      }
    });
    return sparkSession.sqlContext().createDataFrame(rowRDD, schema);
  }

  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row for each partition.
   * 
   * @param beanClazz
   * @return {@code Dataset<Row>}
   * @throws ClassNotFoundException
   */
  public <T> Dataset<Row> mapPartitionToBeanTypeDS(Class<T> beanClazz)
      throws ClassNotFoundException {
    JavaRDD<T> rddDataframe = javaRdd.mapPartitions(new FlatMapFunction<Iterator<byte[]>, T>() {
      @Override
      public Iterator<T> call(Iterator<byte[]> t) throws Exception {
        List<T> tupleList = new ArrayList<T>();
        while (t.hasNext()) {
          hhRDDReader.setByteBuffer(ByteBuffer.wrap(t.next()));
          tupleList.add(getTuple(beanClazz));
        }
        return tupleList.iterator();
      }
    }, true);
    Dataset<Row> dataset =
        sparkSession.sqlContext().createDataFrame(rddDataframe, Class.forName(beanClazz.getName()));
    return dataset;
  }

  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row for each partition. It is supported for schema definition of {@code StructType}.
   * 
   * @param fieldName : It is user defined column name. i.e new String[] {"Col1", "Col2", "Col3",
   *        "Col4", "Col5", "Col6", "Col7", "Col8", "Col9"}
   * @return {@code Dataset<Row>}
   * @throws ClassNotFoundException
   * @throws UnsupportedDataTypeException
   */
  public Dataset<Row> mapPartitionToStructTypeDS(String[] fieldName)
      throws ClassNotFoundException, UnsupportedDataTypeException {
    StructType schema = createSchema(fieldName);
    JavaRDD<Row> rowRDD = javaRdd.mapPartitions(new FlatMapFunction<Iterator<byte[]>, Row>() {
      @Override
      public Iterator<Row> call(Iterator<byte[]> t) throws Exception {
        List<Row> tupleList = new ArrayList<Row>();
        while (t.hasNext()) {
          hhRDDReader.setByteBuffer(ByteBuffer.wrap(t.next()));
          tupleList.add(getRow(t.next()));
        }
        return tupleList.iterator();
      }
    });
    return sparkSession.sqlContext().createDataFrame(rowRDD, schema);
  }

  public Row getRow(byte[] b) throws UnsupportedDataTypeException {
    int columns = hhRDDReader.getFieldDataDescription().getNumberOfDataFields();
    hhRDDReader.setByteBuffer(ByteBuffer.wrap(b));
    Object[] tuple = new Object[columns];
    for (int index = 0; index < columns; index++) {
      Object obj = hhRDDReader.readAtColumn(index);
      if (obj instanceof MutableCharArrayString) {
        tuple[index] = ((MutableCharArrayString) hhRDDReader.readAtColumn(index)).toString();
      } else if (obj instanceof Double) {
        tuple[index] = ((Double) (hhRDDReader.readAtColumn(index)));
      } else if (obj instanceof Integer) {
        tuple[index] = ((Integer) (hhRDDReader.readAtColumn(index)));
      } else if (obj instanceof Byte) {
        tuple[index] = ((Byte) (hhRDDReader.readAtColumn(index)));
      } else if (obj instanceof Character) {
        tuple[index] = ((Character) (hhRDDReader.readAtColumn(index)));
      } else if (obj instanceof Short) {
        tuple[index] = ((Short) (hhRDDReader.readAtColumn(index)));
      } else if (obj instanceof Long) {
        tuple[index] = ((Long) (hhRDDReader.readAtColumn(index)));
      } else if (obj instanceof Float) {
        tuple[index] = ((Float) (hhRDDReader.readAtColumn(index)));
      } else {
        throw new UnsupportedDataTypeException("Invalid data type conversion");
      }
    }
    return RowFactory.create(tuple);
  }

  public StructType createSchema(String[] fieldName) throws UnsupportedDataTypeException {
    List<StructField> fields = new ArrayList<>();
    for (int index = 0; index < hhRDDReader.getFieldDataDescription()
        .getNumberOfDataFields(); index++) {
      DataLocator locator = hhRDDReader.getFieldDataDescription().locateField(index);
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
    return schema;
  }

  private <T> T getTuple(Class<T> beanClazz)
      throws NoSuchFieldException, IllegalAccessException, InstantiationException {
    return new HHTupleType<T>(hhRDDReader) {
      @Override
      public T createTuple() throws InstantiationException, IllegalAccessException {
        return (T) beanClazz.newInstance();
      }
    }.getTuple();
  }

}
