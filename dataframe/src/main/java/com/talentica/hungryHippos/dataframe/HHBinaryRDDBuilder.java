/**
 * byte[]
 * 
 */
package com.talentica.hungryHippos.dataframe;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.activation.UnsupportedDataTypeException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDInfo;
import com.talentica.hungryHippos.rdd.reader.HHRDDBinaryRowReader;
import com.talentica.hungryHippos.sql.HHSparkSession;
import com.talentica.hungryHippos.sql.HHStructField;

/**
 * To create the HHJavaRDD by translating the HH row records to spark recognized RDD row.
 * 
 * @author pooshans
 * @param <T>
 * @param <T>
 * @since 25/01/2017
 *
 */
public class HHBinaryRDDBuilder<T> extends HHRDDBuilder<byte[]> implements Serializable {
  private static final long serialVersionUID = -2899308802854212675L;
  protected HHRDDBinaryRowReader hhRDDReader;

  public HHBinaryRDDBuilder(HHRDDInfo hhrddInfo, HHSparkSession hhSparkSession) {
    super(hhSparkSession);
    hhRDDReader = new HHRDDBinaryRowReader(hhrddInfo.getFieldDataDesc());
  }

  public HHBinaryRDDBuilder(HHRDD hhRdd, HHRDDInfo hhrddInfo,
      HHSparkSession hhSparkSession) {
    super(hhRdd, hhSparkSession);
    hhRDDReader = new HHRDDBinaryRowReader(hhrddInfo.getFieldDataDesc());
  }


  /**
   * It translate the each row to a tuple in map.
   * 
   * @param beanClazz Client provided bean class
   * @return JavaRDD<T>
   */
  public JavaRDD<Object> mapToJavaRDD(Class beanClazz) {
    return javaRdd.map(new Function<byte[], Object>() {
      @Override
      public Object call(byte[] b) throws Exception {
        hhRDDReader.wrap(b);
        return getTuple(beanClazz);
      }
    });
  }

  /**
   * It simply translate each row to JavaRDD of Row type.
   * 
   * @return JavaRDD<Row>
   */
  @Override
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
  @Override
  public JavaRDD<Object> mapPartitionToJavaRDD(Class beanClazz) {
    return javaRdd.mapPartitions(new FlatMapFunction<Iterator<byte[]>, Object>() {
      @Override
      public Iterator<Object> call(Iterator<byte[]> t) throws Exception {
        List<Object> tupleList = new ArrayList<Object>();
        while (t.hasNext()) {
          hhRDDReader.wrap(t.next());
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
  @Override
  public JavaRDD<Row> mapPartitionToJavaRDD() {
    return javaRdd.mapPartitions(new FlatMapFunction<Iterator<byte[]>, Row>() {
      @Override
      public Iterator<Row> call(Iterator<byte[]> t) throws Exception {
        List<Row> tupleList = new ArrayList<Row>();
        while (t.hasNext()) {
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
  @Override
  public StructType getOrCreateSchema() throws UnsupportedDataTypeException {
    StructType schema;
    List<StructField> fields = new ArrayList<StructField>();
    Iterator<HHStructField> hhStructType = hhSparkSession.getHHStructType().iterator();
    while (hhStructType.hasNext()) {
      HHStructField column = hhStructType.next();
      if (!column.isPartOfSqlStmt())
        continue;
      DataLocator locator = hhRDDReader.getFieldDataDescription().locateField(column.getIndex());
      StructField field = null;
      switch (locator.getDataType()) {
        case BYTE:
          field = DataTypes.createStructField(column.getName(), DataTypes.ByteType, true);
          break;
        case CHAR:
          field = DataTypes.createStructField(column.getName(), DataTypes.StringType, true);
          break;
        case SHORT:
          field = DataTypes.createStructField(column.getName(), DataTypes.ShortType, true);
          break;
        case INT:
          field = DataTypes.createStructField(column.getName(), DataTypes.IntegerType, true);
          break;
        case LONG:
          field = DataTypes.createStructField(column.getName(), DataTypes.LongType, true);
          break;
        case FLOAT:
          field = DataTypes.createStructField(column.getName(), DataTypes.FloatType, true);
          break;
        case DOUBLE:
          field = DataTypes.createStructField(column.getName(), DataTypes.DoubleType, true);
          break;
        case STRING:
          field = DataTypes.createStructField(column.getName(), DataTypes.StringType, true);
          break;
        default:
          throw new UnsupportedDataTypeException("Invalid field data type");
      }
      fields.add(field);
    }
    schema = DataTypes.createStructType(fields);
    return schema;
  }

  @Override
  protected Object getTuple(Class beanClazz) throws InstantiationException, IllegalAccessException,
      NoSuchFieldException, SecurityException {
    return new HHTupleType<Class>(hhRDDReader, hhSparkSession) {
      @Override
      public Object createTuple() throws InstantiationException, IllegalAccessException {
        return beanClazz.newInstance();
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
  @Override
  protected Row getRow(byte[] b) throws UnsupportedDataTypeException {
    hhRDDReader.wrap(b);
    Object[] tuple = new Object[hhSparkSession.getHHStructType().size()];
    Iterator<HHStructField> hhStructType = hhSparkSession.getHHStructType().iterator();
    int index = 0;
    while (hhStructType.hasNext()) {
      HHStructField column = hhStructType.next();
      if (!column.isPartOfSqlStmt())
        continue;
      Object obj = hhRDDReader.readAtColumn(column.getIndex());
      if (obj instanceof MutableCharArrayString) {
        tuple[index++] = ((MutableCharArrayString) obj).toString();
      } else if (obj instanceof Double) {
        tuple[index++] = ((Double) (obj));
      } else if (obj instanceof Integer) {
        tuple[index++] = ((Integer) (obj));
      } else if (obj instanceof Byte) {
        tuple[index++] = ((Byte) (obj));
      } else if (obj instanceof Character) {
        tuple[index++] = ((Character) (obj));
      } else if (obj instanceof Short) {
        tuple[index++] = ((Short) (obj));
      } else if (obj instanceof Long) {
        tuple[index++] = ((Long) (obj));
      } else if (obj instanceof Float) {
        tuple[index++] = ((Float) (obj));
      } else {
        throw new UnsupportedDataTypeException("Invalid data type conversion");
      }
    }
    return RowFactory.create(tuple);
  }



}
