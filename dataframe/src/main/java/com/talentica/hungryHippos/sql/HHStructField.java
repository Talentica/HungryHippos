/**
 * 
 */
package com.talentica.hungryHippos.sql;

import java.io.Serializable;

import org.apache.spark.sql.types.DataType;


/**
 * It is the column of the schema and wrapped with the {@code HHStructType} to represent schema
 * definition.
 * 
 * @author pooshans
 *
 */
public class HHStructField implements Serializable {
  private static final long serialVersionUID = 7968184949922361937L;
  private String name;
  private int index;
  private boolean partOfSqlStmt;
  private DataType dataType;

  public HHStructField(String name, int index, boolean partOfSqlStmt) {
    this.name = name;
    this.index = index;
    this.partOfSqlStmt = partOfSqlStmt;
  }

  public HHStructField(String name, int index, boolean partOfSqlStmt, DataType dataTypes) {
    this.name = name;
    this.index = index;
    this.partOfSqlStmt = partOfSqlStmt;
    this.dataType = dataTypes;
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

  public DataType getDataType() {
    return dataType;
  }

  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }

  @Override
  public String toString() {
    return "FieldInfo [name=" + name + ", index=" + index + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || !(o instanceof HHStructField)) {
      return false;
    }
    HHStructField that = (HHStructField) o;
    return name.equals(that.name) && index == that.index;
  }

  @Override
  public int hashCode() {
    int h = 0;
    h = 31 * index + (name != null ? name.hashCode() : 0);
    return h;
  }

}
