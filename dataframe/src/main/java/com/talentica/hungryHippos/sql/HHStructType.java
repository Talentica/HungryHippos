/**
 * 
 */
package com.talentica.hungryHippos.sql;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * It is the basic representation of the schema which is identified by the {@code HHStructField} it
 * wrapped.
 * 
 * @author pooshans
 *
 */
public class HHStructType implements Serializable {

  private static final long serialVersionUID = 1951185415350735839L;
  private Set<HHStructField> hhStructFieldTypeSet;

  public HHStructType() {
    hhStructFieldTypeSet = new HashSet<HHStructField>();
  }

  public HHStructType add(HHStructField hhStructField) {
    hhStructFieldTypeSet.add(hhStructField);
    return this;
  }

  public void clear() {
    hhStructFieldTypeSet.clear();
  }

  public boolean isEmpty() {
    return hhStructFieldTypeSet.isEmpty();
  }

  /**
   * It toggle the status of the column. If the status of the column is false, toggle will makes it
   * true and vice-versa. And now this field will be included as a part of the SQL statement.
   * 
   * @param columnsName
   */
  public void toggleColumnStatus(String[] columnsName) {
    Iterator<HHStructField> colItr = hhStructFieldTypeSet.iterator();
    while (colItr.hasNext()) {
      HHStructField field = colItr.next();
      for (int index = 0; index < columnsName.length; index++) {
        if (field.getName().equalsIgnoreCase(columnsName[index])) {
          field.setPartOfSqlStmt(!field.isPartOfSqlStmt());
          break;
        }
      }
    }
  }

  public Iterator<HHStructField> iterator() {
    return hhStructFieldTypeSet.iterator();
  }

  public int size() {
    return hhStructFieldTypeSet.size();
  }

}
