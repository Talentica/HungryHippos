/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * @author pooshans
 *
 */
public class HHSparkSession extends SparkSession {
  private static final long serialVersionUID = -7510199519029156054L;
  private int selectedColumnsInSQLStmt;
  private Set<FieldInfo> entireFieldInfo;
  private String sqlText;
  protected boolean sqlStmtParsed = false;
  protected LogicalPlan logicalPlan;

  public HHSparkSession(SparkContext sc) {
    super(sc);
  }

  @Override
  public Dataset<Row> sql(String sqlText) {
    return super.sql(this.sqlText = sqlText);
  }

  protected void parseSQLStatement() {
    selectedColumnsInSQLStmt = 0;
    if (sqlText == null)
      throw new RuntimeException("SQL query can not be null");
    if (sqlText.contains("*")) {
      setAllTrue();
    } else {
      Iterator<FieldInfo> fieldInfoItr = entireFieldInfo.iterator();
      while (fieldInfoItr.hasNext()) {
        FieldInfo fieldInfo = fieldInfoItr.next();
        if (fieldInfo.getName() != null && sqlText.contains(fieldInfo.getName())
            && !fieldInfo.isPartOfSqlStmt()) {
          fieldInfo.setPartOfSqlStmt(true);
          selectedColumnsInSQLStmt++;
        }
      }
    }
    sqlStmtParsed = true;
  }

  protected void setLogicalPlan(LogicalPlan logicalPlan) {
    this.logicalPlan = logicalPlan;
  }

  private void setAllTrue() {
    Iterator<FieldInfo> fieldInfoItr = entireFieldInfo.iterator();
    while (fieldInfoItr.hasNext()) {
      FieldInfo fieldInfo = fieldInfoItr.next();
      fieldInfo.setPartOfSqlStmt(true);
      selectedColumnsInSQLStmt++;
    }
  }

  public boolean isSqlStmtParsed() {
    return sqlStmtParsed;
  }

  public void setSqlStmtParsed(boolean sqlStmtParsed) {
    this.sqlStmtParsed = sqlStmtParsed;
  }

  protected void setFieldInfo(Set<FieldInfo> entireFieldInfo) {
    this.entireFieldInfo = entireFieldInfo;
  }

  protected Set<FieldInfo> getFieldInfo() {
    return entireFieldInfo;
  }

  protected int getTotalSQLFields() {
    return selectedColumnsInSQLStmt;
  }

  protected FieldInfo getFieldInfoInstance(String name, int index, boolean partOfSqlStmt) {
    return new FieldInfo(name, index, partOfSqlStmt);
  }

  protected class FieldInfo implements Serializable {
    private static final long serialVersionUID = 9070250323611650296L;
    private String name;
    private int index;
    private boolean partOfSqlStmt;

    public FieldInfo(String name, int index, boolean partOfSqlStmt) {
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
      if (o == null || !(o instanceof FieldInfo)) {
        return false;
      }
      FieldInfo that = (FieldInfo) o;
      return name.equals(that.name) && index == that.index;
    }

    @Override
    public int hashCode() {
      int h = 0;
      h = 31 * index + (name != null ? name.hashCode() : 0);
      return h;
    }

  }

}
