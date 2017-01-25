/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Comparator;

/**
 * This annotation is used to make the field of the class in particular order guided by this index
 * while fetching the fields by reflection.
 * 
 * @author pooshans
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface HHFieldOrder {
  int index();

}


class HHFieldOrderUtil {
  public static Field[] getOrderedDeclaredFields(Class<?> clazz) {
    Field[] fields = clazz.getDeclaredFields();
    Arrays.sort(fields, new Comparator<Field>() {
      @Override
      public int compare(Field field1, Field field2) {
        HHFieldOrder hhOdr1 = field1.getAnnotation(HHFieldOrder.class);
        HHFieldOrder hhOdr2 = field2.getAnnotation(HHFieldOrder.class);
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
