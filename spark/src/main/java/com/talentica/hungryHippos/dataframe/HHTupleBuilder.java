package com.talentica.hungryHippos.dataframe;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

/**
 * To create new tuple for each row of the records to get ready for SQL query..
 * 
 * @author pooshans
 *
 */
public class HHTupleBuilder implements Serializable {
  private static final long serialVersionUID = -9090118505722532509L;
  private static HHTuple tupleObj;
  private final static String KEY_PRIFIX = "key";

  public static HHTuple getHHTuple(HHRDDRowReader hhrddRowReader) throws NoSuchFieldException,
      SecurityException, IllegalArgumentException, IllegalAccessException, NoSuchMethodException,
      InvocationTargetException, InstantiationException, CloneNotSupportedException {
    return createTuple(hhrddRowReader);
  }

  /**
   * To create the new Tuple for each row.
   * 
   * @param hhrddRowReader
   * @return HHTuple
   * @throws NoSuchFieldException
   * @throws CloneNotSupportedException
   * @throws IllegalAccessException
   */
  private static HHTuple createTuple(HHRDDRowReader hhrddRowReader)
      throws NoSuchFieldException, CloneNotSupportedException, IllegalAccessException {
    int columns = hhrddRowReader.getFieldDataDescription().getNumberOfDataFields();
    HHTuple tuple = createEmptyTuple(hhrddRowReader.getFieldDataDescription());
    fillupEmptyTuple(hhrddRowReader, columns, tuple);
    return tuple;
  }

  /**
   * To populate the existing empty tuple with under-laying row in process. It also convert the
   * binary format of the particular to corresponding data type object for each column of under
   * processing row.
   * 
   * @param hhrddRowReader
   * @param columns
   * @param tuple
   * @throws NoSuchFieldException
   * @throws IllegalAccessException
   */
  private static void fillupEmptyTuple(HHRDDRowReader hhrddRowReader, int columns, HHTuple tuple)
      throws NoSuchFieldException, IllegalAccessException {
    Class<?> clazz = tuple.getClass();
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

  /**
   * To create the empty tuple and do first time dataType validation internally with the specified
   * in {@code ShardingClientConfig} or sharding-client.xml and later on it just return the clone of
   * the existing one to avoid regular validation.
   * 
   * @param fieldTypeArrayDataDescription
   * @return HHTuple
   * @throws NoSuchFieldException
   * @throws CloneNotSupportedException
   */
  private static HHTuple createEmptyTuple(
      FieldTypeArrayDataDescription fieldTypeArrayDataDescription)
      throws NoSuchFieldException, CloneNotSupportedException {
    HHTuple tuple;
    if (tupleObj == null) {
      tupleObj = new HHTuple(fieldTypeArrayDataDescription);
      tuple = tupleObj;
    } else {
      tuple = (HHTuple) tupleObj.clone();
    }
    return tuple;
  }

}
