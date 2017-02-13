/**
 * 
 */
package com.talentica.hungryHippos.rdd.reader;

import java.io.Serializable;

/**
 * @author pooshans
 *
 */
public interface HHRowReader<T> extends Serializable{

  HHRowReader<T> wrap(T row);
  
  Object readAtColumn(int index);

}
