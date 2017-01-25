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
 * To create the HHJavaRDD by translating the HH row records to spark recognized RDD row.
 * 
 * @author pooshans
 *
 */
public class HHJavaRDDBuilder implements Serializable {
  private static final long serialVersionUID = -2899308802854212675L;
  protected JavaRDD<byte[]> javaRdd;
  protected SparkSession sparkSession;
  protected HHRDDRowReader hhRDDReader;

  public HHJavaRDDBuilder(HHRDDInfo hhrddInfo) {
    hhRDDReader = new HHRDDRowReader(hhrddInfo.getFieldDataDesc());
  }

  public HHJavaRDDBuilder(HHRDD hhRdd, HHRDDInfo hhrddInfo, SparkSession sparkSession) {
    this.javaRdd = hhRdd.toJavaRDD();
    this.sparkSession = sparkSession;
    hhRDDReader = new HHRDDRowReader(hhrddInfo.getFieldDataDesc());
  }

  /**
   * It translate the each row to a tuple in map.
   * 
   * @param beanClazz Client provided bean class
   * @return JavaRDD<T>
   */
  public <T> JavaRDD<T> mapToJavaRDD(Class<T> beanClazz) {
    return javaRdd.map(new Function<byte[], T>() {
      @Override
      public T call(byte[] b) throws Exception {
        hhRDDReader.setByteBuffer(ByteBuffer.wrap(b));
        return getTuple(beanClazz);
      }
    });
  }

  /**
   * It simply translate each row to JavaRDD of Row type.
   * 
   * @return JavaRDD<Row>
   */
  public JavaRDD<Row> mapToJavaRDD() {
    return javaRdd.map(new Function<byte[], Row>() {
      @Override
      public Row call(byte[] b) throws Exception {
        return getRow(b);
      }
    });
  }

  /**
   * It translate the map partition to JavaRDD of client provided bean class.
   * 
   * @param beanClazz
   * @return JavaRDD<T>
   */
  public <T> JavaRDD<T> mapPartitionToJavaRDD(Class<T> beanClazz) {
    return javaRdd.mapPartitions(new FlatMapFunction<Iterator<byte[]>, T>() {
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
  }

  /**
   * It simply translate the map partition to JavaRDD of Row type.
   * 
   * @return
   */
  public JavaRDD<Row> mapPartitionToJavaRDD() {
    return javaRdd.mapPartitions(new FlatMapFunction<Iterator<byte[]>, Row>() {
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
  }


  /**
   * It is used to create the schema by providing the schema definition.
   * 
   * @param fieldName
   * @return StructType
   * @throws UnsupportedDataTypeException
   */
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

  protected <T> T getTuple(Class<T> beanClazz)
      throws NoSuchFieldException, IllegalAccessException, InstantiationException {
    return new HHTupleType<T>(hhRDDReader) {
      @Override
      public T createTuple() throws InstantiationException, IllegalAccessException {
        return (T) beanClazz.newInstance();
      }
    }.getTuple();
  }


  /**
   * It is used to convert the byte representation of the row into Spark recognized Row.
   * 
   * @param b
   * @return Row
   * @throws UnsupportedDataTypeException
   */
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


}
