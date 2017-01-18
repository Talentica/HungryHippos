package com.talentica.hdfs.spark.binary.dataframe;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.rdd.reader.HHRDDRowReader;

public class SchemaBuilder implements Serializable {
  private static final long serialVersionUID = -9090118505722532509L;
  private SchemaBean schemaBean;

  public SchemaBuilder(HHRDDRowReader hhrddRowReader, int columns)
      throws NoSuchFieldException, SecurityException, IllegalArgumentException,
      IllegalAccessException, NoSuchMethodException, InvocationTargetException {
    schemaBean = new SchemaBean();
    Class<?> clazz = schemaBean.getClass();
    for (int col = 0; col < columns; col++) {
      DataLocator locator = hhrddRowReader.getFieldDataDescription().locateField(col);
      Field column = clazz.getDeclaredField("column" + (col + 1));
      column.setAccessible(true);
      switch (locator.getDataType()) {
        case BYTE:
          column.set(schemaBean,
              Byte.valueOf(hhrddRowReader.getByteBuffer().get(locator.getOffset())));
          break;
        case CHAR:
          column.set(schemaBean,
              Character.valueOf(hhrddRowReader.getByteBuffer().getChar(locator.getOffset())));
          break;
        case SHORT:
          column.set(schemaBean,
              Short.valueOf(hhrddRowReader.getByteBuffer().getShort(locator.getOffset())));
          break;
        case INT:
          column.set(schemaBean,
              Integer.valueOf(hhrddRowReader.getByteBuffer().getInt(locator.getOffset())));
          break;
        case LONG:
          column.set(schemaBean,
              Long.valueOf(hhrddRowReader.getByteBuffer().getLong(locator.getOffset())));
          break;
        case FLOAT:
          column.set(schemaBean,
              Float.valueOf(hhrddRowReader.getByteBuffer().getFloat(locator.getOffset())));
          break;
        case DOUBLE:
          column.set(schemaBean,
              Double.valueOf(hhrddRowReader.getByteBuffer().getDouble(locator.getOffset())));
          break;
        case STRING:
          column.set(schemaBean, hhrddRowReader.readValueString(col).toString());
          break;
        default:
          throw new RuntimeException("Invalid data type");
      }
    }
  }

  public SchemaBean getSchemaBean() {
    return schemaBean;
  }
}
