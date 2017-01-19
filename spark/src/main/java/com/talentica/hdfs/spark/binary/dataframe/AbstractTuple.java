/**
 * 
 */
package com.talentica.hdfs.spark.binary.dataframe;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.DataLocator.DataType;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;

/**
 * @author pooshans
 *
 */
public abstract class AbstractTuple implements Cloneable{

  protected List<DataType> dataType = new ArrayList<DataType>();
  private FieldTypeArrayDataDescription dataDescription;

  public AbstractTuple(FieldTypeArrayDataDescription dataDescription) {
    this.dataDescription = dataDescription;
    for (int col = 0; col < dataDescription.getNumberOfDataFields(); col++) {
      DataLocator locator = dataDescription.locateField(col);
      dataType.add(locator.getDataType());
    }
  }

  public Object clone() throws CloneNotSupportedException{
    return super.clone();
  }
  protected void defaultValidation(Class<?> clazz) throws NoSuchFieldException, SecurityException {
    for (int index = 0; index < dataType.size(); index++) {
      DataLocator locator = dataDescription.locateField(index);
      Field column = clazz.getDeclaredField("key" + (index + 1));
      switch (locator.getDataType()) {
        case BYTE:
          if (!column.getGenericType().getTypeName().equals(Byte.TYPE.getName()))
            throw new RuntimeException("Invalid data type");
          break;
        case CHAR:
          if (!column.getGenericType().getTypeName().equals(Character.TYPE.getName()))
            throw new RuntimeException("Invalid data type");
          break;
        case SHORT:
          if (!column.getGenericType().getTypeName().equals(Short.TYPE.getName()))
            throw new RuntimeException("Invalid data type");
          break;
        case INT:
          if (!column.getGenericType().getTypeName().equals(Integer.TYPE.getName()))
            throw new RuntimeException("Invalid data type");
          break;
        case LONG:
          if (!column.getGenericType().getTypeName().equals(Long.TYPE.getName()))
            throw new RuntimeException("Invalid data type");
          break;
        case FLOAT:
          if (!column.getGenericType().getTypeName().equals(Float.TYPE.getName()))
            throw new RuntimeException("Invalid data type");
          break;
        case DOUBLE:
          if (!column.getGenericType().getTypeName().equals(Double.TYPE.getName()))
            throw new RuntimeException("Invalid data type");
          break;
        case STRING:
          if (!column.getGenericType().getTypeName().equals(String.class.getName()))
            throw new RuntimeException("Invalid data type");
          break;
        default:
          throw new RuntimeException("Invalid data type");
      }

    }
  }

  protected abstract void doDataTypeValidation() throws NoSuchFieldException, SecurityException;
}
