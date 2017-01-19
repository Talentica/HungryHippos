/**
 * 
 */
package com.talentica.hungryHippos.spark.dataframe;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.DataLocator.DataType;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;

/**
 * This class is required to do default validation data type for each column. Users are required to
 * extend this class for doing data type validation at the time of object creation.
 * 
 * @author pooshans
 *
 */
public class HHTupleType<T> implements Cloneable {

  protected List<DataType> dataType = new ArrayList<DataType>();
  private FieldTypeArrayDataDescription dataDescription;
  private final static String KEY_PRIFIX = "key";

  public HHTupleType(FieldTypeArrayDataDescription dataDescription)
      throws NoSuchFieldException, SecurityException {
    this.dataDescription = dataDescription;
    for (int col = 0; col < dataDescription.getNumberOfDataFields(); col++) {
      DataLocator locator = dataDescription.locateField(col);
      dataType.add(locator.getDataType());
    }
    defaultValidation(this);
  }

  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  /**
   * This is the default validation provided to the data type of the columns.
   * 
   * @param clazz
   * @throws NoSuchFieldException
   * @throws SecurityException
   */
  protected <T> void defaultValidation(T clazz) throws NoSuchFieldException, SecurityException {
    for (int index = 0; index < dataType.size(); index++) {
      DataLocator locator = dataDescription.locateField(index);
      Field column = clazz.getClass().getDeclaredField("key" + (index + 1));
      switch (locator.getDataType()) {
        case BYTE:
          if (!column.getGenericType().getTypeName().equals(Byte.TYPE.getName()))
            throw new RuntimeException(
                "Invalid data type for column :: " + KEY_PRIFIX + (index + 1));
          break;
        case CHAR:
          if (!column.getGenericType().getTypeName().equals(Character.TYPE.getName()))
            throw new RuntimeException(
                "Invalid data type for column :: " + KEY_PRIFIX + (index + 1));
          break;
        case SHORT:
          if (!column.getGenericType().getTypeName().equals(Short.TYPE.getName()))
            throw new RuntimeException(
                "Invalid data type for column :: " + KEY_PRIFIX + (index + 1));
          break;
        case INT:
          if (!column.getGenericType().getTypeName().equals(Integer.TYPE.getName()))
            throw new RuntimeException(
                "Invalid data type for column :: " + KEY_PRIFIX + (index + 1));
          break;
        case LONG:
          if (!column.getGenericType().getTypeName().equals(Long.TYPE.getName()))
            throw new RuntimeException(
                "Invalid data type for column :: " + KEY_PRIFIX + (index + 1));
          break;
        case FLOAT:
          if (!column.getGenericType().getTypeName().equals(Float.TYPE.getName()))
            throw new RuntimeException(
                "Invalid data type for column :: " + KEY_PRIFIX + (index + 1));
          break;
        case DOUBLE:
          if (!column.getGenericType().getTypeName().equals(Double.TYPE.getName()))
            throw new RuntimeException(
                "Invalid data type for column :: " + KEY_PRIFIX + (index + 1));
          break;
        case STRING:
          if (!column.getGenericType().getTypeName().equals(String.class.getName()))
            throw new RuntimeException(
                "Invalid data type for column :: " + KEY_PRIFIX + (index + 1));
          break;
        default:
          throw new RuntimeException("Invalid data type for column :: " + KEY_PRIFIX + (index + 1));
      }

    }
  }
}
