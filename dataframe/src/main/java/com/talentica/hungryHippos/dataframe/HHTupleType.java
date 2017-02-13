/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.DataLocator.DataType;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.rdd.reader.HHBinaryRowReader;
import com.talentica.hungryHippos.rdd.reader.HHTextRowReader;
import com.talentica.hungryHippos.rdd.reader.HHRowReader;
import com.talentica.hungryHippos.sql.HHSparkSession;
import com.talentica.hungryHippos.sql.HHStructField;

/**
 * This class is required to do default validation data type for each column. Users are required to
 * Instantiate this class to create the tuple for each row.
 * 
 * @author pooshans
 * @since 25/01/2017
 *
 */
public abstract class HHTupleType<T> implements Cloneable,Serializable {
  private static final long serialVersionUID = -8461246214626349933L;
  protected List<DataType> dataType = new ArrayList<DataType>();
  private FieldTypeArrayDataDescription dataDescription;
  private HHRowReader<?> hhRowReader;
  private HHSparkSession<?> hhSparkSession;
  protected Object tuple;
  private boolean isBinaryRowReader = false;

  /**
   * Constructor of the HHTupleType
   * 
   * @param hhrddRowReader
   * @throws NoSuchFieldException
   * @throws SecurityException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public HHTupleType(HHRowReader hhRowReader, HHSparkSession hhSparkSession)
      throws NoSuchFieldException, SecurityException, IllegalAccessException,
      InstantiationException {
    this.hhRowReader = hhRowReader;
    this.hhSparkSession = hhSparkSession;
    if (hhRowReader instanceof HHBinaryRowReader) {
      HHBinaryRowReader reader = (HHBinaryRowReader) hhRowReader;
      this.dataDescription = reader.getFieldDataDescription();
      for (int col = 0; col < dataDescription.getNumberOfDataFields(); col++) {
        DataLocator locator = dataDescription.locateField(col);
        dataType.add(locator.getDataType());
      }
      isBinaryRowReader = true;
    }
    defaultTypeValidation();
    prepareTuple();
  }

  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  public Object getTuple() throws InstantiationException, IllegalAccessException {
    return this.tuple;
  }

  public abstract Object createTuple() throws InstantiationException, IllegalAccessException;

  private static boolean isDataTypeValidated = false;

  /**
   * This is the default validation provided to the data type of the columns.
   * 
   * @param clazz
   * @throws NoSuchFieldException
   * @throws SecurityException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  protected void defaultTypeValidation() throws NoSuchFieldException, SecurityException,
      InstantiationException, IllegalAccessException {
    tuple = createTuple();
    if (isDataTypeValidated || !isBinaryRowReader)
      return;
    Field[] fields = getOrderedDeclaredFields(tuple.getClass());
    Iterator<HHStructField> colItr = hhSparkSession.getHHStructType().iterator();
    while (colItr.hasNext()) {
      HHStructField col = colItr.next();
      if (!col.isPartOfSqlStmt())
        continue;
      DataLocator locator = dataDescription.locateField(col.getIndex());
      Field column = fields[col.getIndex()];
      switch (locator.getDataType()) {
        case BYTE:
          if (!column.getGenericType().getTypeName().equals(Byte.TYPE.getName()))
            throw new RuntimeException("Invalid data type for field :: " + column.getName()
                + ". Expecting data type :: " + Byte.TYPE.getName());
          break;
        case CHAR:
          if (!column.getGenericType().getTypeName().equals(Character.TYPE.getName()))
            throw new RuntimeException("Invalid data type for field :: " + column.getName()
                + ". Expecting data type :: " + Character.TYPE.getName());
          break;
        case SHORT:
          if (!column.getGenericType().getTypeName().equals(Short.TYPE.getName()))
            throw new RuntimeException("Invalid data type for field :: " + column.getName()
                + ". Expecting data type :: " + Short.TYPE.getName());
          break;
        case INT:
          if (!column.getGenericType().getTypeName().equals(Integer.TYPE.getName()))
            throw new RuntimeException("Invalid data type for field :: " + column.getName()
                + ". Expecting data type :: " + Integer.TYPE.getName());
          break;
        case LONG:
          if (!column.getGenericType().getTypeName().equals(Long.TYPE.getName()))
            throw new RuntimeException("Invalid data type for field :: " + column.getName()
                + ". Expecting data type :: " + Long.TYPE.getName());
          break;
        case FLOAT:
          if (!column.getGenericType().getTypeName().equals(Float.TYPE.getName()))
            throw new RuntimeException("Invalid data type for field :: " + column.getName()
                + ". Expecting data type :: " + Float.TYPE.getName());
          break;
        case DOUBLE:
          if (!column.getGenericType().getTypeName().equals(Double.TYPE.getName()))
            throw new RuntimeException("Invalid data type for field :: " + column.getName()
                + ". Expecting data type :: " + Double.TYPE.getName());
          break;
        case STRING:
          if (!column.getGenericType().getTypeName().equals(String.class.getName()))
            throw new RuntimeException("Invalid data type for field :: " + column.getName()
                + ". Expecting data type :: " + String.class.getName());
          break;
        default:
          throw new RuntimeException("Invalid data type for field :: " + column.getName());
      }

    }
    isDataTypeValidated = true;
  }

  /**
   * To make tuple ready with complete information of the row.
   * 
   * @throws NoSuchFieldException
   * @throws IllegalAccessException
   */
  private void prepareTuple() throws NoSuchFieldException, IllegalAccessException {
    Class<?> clazz = tuple.getClass();
    Field[] fields = getOrderedDeclaredFields(clazz);
    Iterator<HHStructField> fieldInfoItr = hhSparkSession.getHHStructType().iterator();
    HHBinaryRowReader<byte[]> binaryRowReader = null;
    HHTextRowReader<String> textRowReader = null;

    if (isBinaryRowReader) {
      binaryRowReader = (HHBinaryRowReader<byte[]>) hhRowReader;
    } else {
      textRowReader = (HHTextRowReader<String>) hhRowReader;
    }


    while (fieldInfoItr.hasNext()) {
      HHStructField columnInfo = fieldInfoItr.next();
      if (!columnInfo.isPartOfSqlStmt())
        continue;
      DataLocator locator;
      Field column = fields[columnInfo.getIndex()];
      column.setAccessible(true);
      if (isBinaryRowReader) {
        locator = binaryRowReader.getFieldDataDescription().locateField(columnInfo.getIndex());
        switch (locator.getDataType()) {
          case BYTE:
            column.set(tuple,
                Byte.valueOf(binaryRowReader.getByteBuffer().get(locator.getOffset())));
            break;
          case CHAR:
            column.set(tuple,
                Character.valueOf(binaryRowReader.getByteBuffer().getChar(locator.getOffset())));
            break;
          case SHORT:
            column.set(tuple,
                Short.valueOf(binaryRowReader.getByteBuffer().getShort(locator.getOffset())));
            break;
          case INT:
            column.set(tuple,
                Integer.valueOf(binaryRowReader.getByteBuffer().getInt(locator.getOffset())));
            break;
          case LONG:
            column.set(tuple,
                Long.valueOf(binaryRowReader.getByteBuffer().getLong(locator.getOffset())));
            break;
          case FLOAT:
            column.set(tuple,
                Float.valueOf(binaryRowReader.getByteBuffer().getFloat(locator.getOffset())));
            break;
          case DOUBLE:
            column.set(tuple,
                Double.valueOf(binaryRowReader.getByteBuffer().getDouble(locator.getOffset())));
            break;
          case STRING:
            column.set(tuple, binaryRowReader.readValueString(columnInfo.getIndex()).toString());
            break;
          default:
            throw new RuntimeException("Invalid data type");
        }
      } else {
        column.set(tuple, textRowReader.readAtColumn(columnInfo.getIndex()));
      }
    }

  }

  private static Field[] getOrderedDeclaredFields(Class<?> clazz) {
    Field[] fields = clazz.getDeclaredFields();
    Arrays.sort(fields, new Comparator<Field>() {
      @Override
      public int compare(Field field1, Field field2) {
        HHField hhOdr1 = field1.getAnnotation(HHField.class);
        HHField hhOdr2 = field2.getAnnotation(HHField.class);
        if (hhOdr1 != null && hhOdr2 != null) {
          return hhOdr1.index() - hhOdr2.index();
        } else if (hhOdr1 != null && hhOdr2 == null) {
          return -1;
        } else if (hhOdr1 == null && hhOdr2 != null) {
          return 1;
        }
        return field1.getName().compareTo(field2.getName());
      }
    });
    return fields;
  }

}
