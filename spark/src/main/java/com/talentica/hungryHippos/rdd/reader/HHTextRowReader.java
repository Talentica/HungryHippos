/**
 * 
 */
package com.talentica.hungryHippos.rdd.reader;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author pooshans
 *
 */
public class HHTextRowReader<T> implements Serializable, HHRowReader<String> {

  private static final long serialVersionUID = 6893222902757419858L;

  private Map<Integer, String> cache;

  private String row;

  private final static char DELIMITER = ',';

  public HHTextRowReader() {
    cache = new HashMap<Integer, String>();
  }

  @Override
  public HHRowReader<String> wrap(String row) {
    this.row = row;
    cache.clear();
    return this;
  }

  @Override
  public Object readAtColumn(int index) {
    String token = cache.get(index);
    if (token != null) {
      return token;
    }
    int idx = row.indexOf(DELIMITER);
    int lastindex = 0;
    int count = 0;
    while (idx >= -1) {
      if (count == index) {
        cache.put(index,
            row.substring(lastindex, idx == -1 ? (row.length()) : row.indexOf(DELIMITER, idx)));
        break;
      }
      lastindex = idx + 1;
      idx = row.indexOf(DELIMITER, lastindex);
      count++;
    }
    return cache.get(index);
  }

}
