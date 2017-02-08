/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import java.io.Serializable;


/**
 * @author pooshans
 *
 */
public class Column implements Serializable {
  private static final long serialVersionUID = 9070250323611650296L;
  private String name;
  private int index;
  private boolean partOfSqlStmt;

  public Column(String name, int index, boolean partOfSqlStmt) {
    this.name = name;
    this.index = index;
    this.partOfSqlStmt = partOfSqlStmt;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public boolean isPartOfSqlStmt() {
    return partOfSqlStmt;
  }

  public void setPartOfSqlStmt(boolean partOfSqlStmt) {
    this.partOfSqlStmt = partOfSqlStmt;
  }

  @Override
  public String toString() {
    return "FieldInfo [name=" + name + ", index=" + index + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof Column)) {
      return false;
    }
    Column that = (Column) o;
    return name.equals(that.name) && index == that.index;
  }

  @Override
  public int hashCode() {
    int h = 0;
    h = 31 * index + (name != null ? name.hashCode() : 0);
    return h;
  }

}
