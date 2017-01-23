/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.DataLocator.DataType;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

/**
 * This class is required to do default validation data type for each column. Users are required to
 * Instantiate this class to create the tuple for each row.
 * 
 * @author pooshans
 *
 */
public abstract class HHTupleType<T> implements Cloneable {

  protected List<DataType> dataType = new ArrayList<DataType>();
  private FieldTypeArrayDataDescription dataDescription;
  private final static String KEY_PRIFIX = "key";
  private HHRDDRowReader hhrddRowReader;
  protected T tuple;

  public HHTupleType(HHRDDRowReader hhrddRowReader)
      throws NoSuchFieldException, SecurityException, IllegalAccessException {
    this.hhrddRowReader = hhrddRowReader;
    this.dataDescription = hhrddRowReader.getFieldDataDescription();
    for (int col = 0; col < dataDescription.getNumberOfDataFields(); col++) {
      DataLocator locator = dataDescription.locateField(col);
      dataType.add(locator.getDataType());
    }
    defaultTypleDatatypeValidation();
    prepareTuple();
  }

  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  public T getTuple() {
    return this.tuple;
  }

  protected abstract T createTuple();

  private static boolean isDataTypeValidated = false;

  /**
   * This is the default validation provided to the data type of the columns.
   * 
   * @param clazz
   * @throws NoSuchFieldException
   * @throws SecurityException
   */
  protected void defaultTypleDatatypeValidation() throws NoSuchFieldException, SecurityException {
    tuple = createTuple();
    if (isDataTypeValidated)
      return;
    int noOfFields = tuple.getClass().getDeclaredFields().length;
    if (noOfFields != dataType.size()) {
      throw new RuntimeException("Number of fields mismatch defined in Tuple.");
    }
    for (int index = 0; index < dataType.size(); index++) {
      DataLocator locator = dataDescription.locateField(index);
      Field column = tuple.getClass().getDeclaredField("key" + (index + 1));
      switch (locator.getDataType()) {
        case BYTE:
          if (!column.getGenericType().getTypeName().equals(Byte.TYPE.getName()))
            throw new RuntimeException("Invalid data type for column :: " + KEY_PRIFIX + (index + 1)
                + ". Expecting data type :: " + Byte.TYPE.getName());
          break;
        case CHAR:
          if (!column.getGenericType().getTypeName().equals(Character.TYPE.getName()))
            throw new RuntimeException("Invalid data type for column :: " + KEY_PRIFIX + (index + 1)
                + ". Expecting data type :: " + Character.TYPE.getName());
          break;
        case SHORT:
          if (!column.getGenericType().getTypeName().equals(Short.TYPE.getName()))
            throw new RuntimeException("Invalid data type for column :: " + KEY_PRIFIX + (index + 1)
                + ". Expecting data type :: " + Short.TYPE.getName());
          break;
        case INT:
          if (!column.getGenericType().getTypeName().equals(Integer.TYPE.getName()))
            throw new RuntimeException("Invalid data type for column :: " + KEY_PRIFIX + (index + 1)
                + ". Expecting data type :: " + Integer.TYPE.getName());
          break;
        case LONG:
          if (!column.getGenericType().getTypeName().equals(Long.TYPE.getName()))
            throw new RuntimeException("Invalid data type for column :: " + KEY_PRIFIX + (index + 1)
                + ". Expecting data type :: " + Long.TYPE.getName());
          break;
        case FLOAT:
          if (!column.getGenericType().getTypeName().equals(Float.TYPE.getName()))
            throw new RuntimeException("Invalid data type for column :: " + KEY_PRIFIX + (index + 1)
                + ". Expecting data type :: " + Float.TYPE.getName());
          break;
        case DOUBLE:
          if (!column.getGenericType().getTypeName().equals(Double.TYPE.getName()))
            throw new RuntimeException("Invalid data type for column :: " + KEY_PRIFIX + (index + 1)
                + ". Expecting data type :: " + Double.TYPE.getName());
          break;
        case STRING:
          if (!column.getGenericType().getTypeName().equals(String.class.getName()))
            throw new RuntimeException("Invalid data type for column :: " + KEY_PRIFIX + (index + 1)
                + ". Expecting data type :: " + String.class.getName());
          break;
        default:
          throw new RuntimeException("Invalid data type for column :: " + KEY_PRIFIX + (index + 1));
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
    int columns = hhrddRowReader.getFieldDataDescription().getNumberOfDataFields();
    for (int col = 0; col < columns; col++) {
      DataLocator locator = hhrddRowReader.getFieldDataDescription().locateField(col);
      Field column = clazz.getDeclaredField(KEY_PRIFIX + (col + 1));
      column.setAccessible(true);
      switch (locator.getDataType()) {
        case BYTE:
          column.set(tuple, Byte.valueOf(hhrddRowReader.getByteBuffer().get(locator.getOffset())));
          break;
        case CHAR:
          column.set(tuple,
              Character.valueOf(hhrddRowReader.getByteBuffer().getChar(locator.getOffset())));
          break;
        case SHORT:
          column.set(tuple,
              Short.valueOf(hhrddRowReader.getByteBuffer().getShort(locator.getOffset())));
          break;
        case INT:
          column.set(tuple,
              Integer.valueOf(hhrddRowReader.getByteBuffer().getInt(locator.getOffset())));
          break;
        case LONG:
          column.set(tuple,
              Long.valueOf(hhrddRowReader.getByteBuffer().getLong(locator.getOffset())));
          break;
        case FLOAT:
          column.set(tuple,
              Float.valueOf(hhrddRowReader.getByteBuffer().getFloat(locator.getOffset())));
          break;
        case DOUBLE:
          column.set(tuple,
              Double.valueOf(hhrddRowReader.getByteBuffer().getDouble(locator.getOffset())));
          break;
        case STRING:
          column.set(tuple, hhrddRowReader.readValueString(col).toString());
          break;
        default:
          throw new RuntimeException("Invalid data type");
      }
    }

  }
}
