/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import com.talentica.hungryHippos.dataframe.HHSparkSession.FieldInfo;
import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDInfo;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

/**
 * To create the HHJavaRDD by translating the HH row records to spark recognized RDD row.
 * 
 * @author pooshans
 * @param <T>
 * @param <T>
 * @since 25/01/2017
 *
 */
public class HHJavaRDDBuilder<T> implements Serializable {
  private static final long serialVersionUID = -2899308802854212675L;
  protected JavaRDD<byte[]> javaRdd;
  protected HHRDDRowReader hhRDDReader;
  protected HHSparkSession hhSparkSession;

  public HHJavaRDDBuilder(HHRDDInfo hhrddInfo, HHSparkSession hhSparkSession) {
    hhRDDReader = new HHRDDRowReader(hhrddInfo.getFieldDataDesc());
    this.hhSparkSession = hhSparkSession;
  }

  public HHJavaRDDBuilder(HHRDD hhRdd, HHRDDInfo hhrddInfo, HHSparkSession hhSparkSession) {
    this.javaRdd = hhRdd.toJavaRDD();
    hhRDDReader = new HHRDDRowReader(hhrddInfo.getFieldDataDesc());
    this.hhSparkSession = hhSparkSession;
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

  private StructType schema = null;
  private Set<FieldInfo> entireFieldInfo = new HashSet<FieldInfo>();

  /**
   * It is used to create the schema by providing the schema definition.
   * 
   * @param fieldName
   * @return StructType
   * @throws UnsupportedDataTypeException
   */
  public StructType getOrCreateSchema(String[] fieldName) throws UnsupportedDataTypeException {
    List<StructField> fields;
    if (schema != null) {
      return schema;
    } else {
      fields = new ArrayList<StructField>();
      entireFieldInfo.clear();
    }
    for (int index = 0; index < hhRDDReader.getFieldDataDescription()
        .getNumberOfDataFields(); index++) {
      String name = fieldName[index];
      entireFieldInfo.add(hhSparkSession.getFieldInfoInstance(name, index, false));
      if (name == null) {
        continue;
      }
      DataLocator locator = hhRDDReader.getFieldDataDescription().locateField(index);
      StructField field = null;
      switch (locator.getDataType()) {
        case BYTE:
          field = DataTypes.createStructField(name, DataTypes.ByteType, true);
          break;
        case CHAR:
          field = DataTypes.createStructField(name, DataTypes.StringType, true);
          break;
        case SHORT:
          field = DataTypes.createStructField(name, DataTypes.ShortType, true);
          break;
        case INT:
          field = DataTypes.createStructField(name, DataTypes.IntegerType, true);
          break;
        case LONG:
          field = DataTypes.createStructField(name, DataTypes.LongType, true);
          break;
        case FLOAT:
          field = DataTypes.createStructField(name, DataTypes.FloatType, true);
          break;
        case DOUBLE:
          field = DataTypes.createStructField(name, DataTypes.DoubleType, true);
          break;
        case STRING:
          field = DataTypes.createStructField(name, DataTypes.StringType, true);
          break;
        default:
          throw new UnsupportedDataTypeException("Invalid field data type");
      }
      fields.add(field);
    }
    hhSparkSession.setFieldInfo(entireFieldInfo);
    schema = DataTypes.createStructType(fields);
    return schema;
  }


  protected <T> T getTuple(Class<T> beanClazz) throws NoSuchFieldException, IllegalAccessException,
      InstantiationException, CloneNotSupportedException {
    return new HHTupleType<T>(hhRDDReader, hhSparkSession) {
      @Override
      public T createTuple() throws InstantiationException, IllegalAccessException {
        return beanClazz.newInstance();
      }
    }.getTuple();
  }

  boolean init = false;

  /**
   * It is used to convert the byte representation of the row into Spark recognized Row.
   * 
   * @param b
   * @return Row
   * @throws UnsupportedDataTypeException
   */
  public Row getRow(byte[] b) throws UnsupportedDataTypeException {
    if (!hhSparkSession.isSqlStmtParsed()) {
      hhSparkSession.parseSQLStatement();
    }
    hhRDDReader.wrap(b);
    Object[] tuple = new Object[hhSparkSession.getTotalSQLFields()];
    Iterator<FieldInfo> fieldInfoItr = entireFieldInfo.iterator();
    int index = 0;
    while (fieldInfoItr.hasNext()) {
      FieldInfo fieldInfo = fieldInfoItr.next();
      if (!fieldInfo.isPartOfSqlStmt()) {
        continue;
      }
      Object obj = hhRDDReader.readAtColumn(fieldInfo.getIndex());
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
