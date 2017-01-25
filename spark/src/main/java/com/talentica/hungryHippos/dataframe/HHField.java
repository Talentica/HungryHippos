/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is used to make the property of the class in particular order guided by this indexing
 * while fetching the fields by reflection.
 * 
 * @author pooshans
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface HHField {
  int index();
}
