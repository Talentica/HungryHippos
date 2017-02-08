/**
 * 
 */
package com.talentica.hungryHippos.dataframe;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;

import javax.activation.UnsupportedDataTypeException;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import com.talentica.hungryHippos.rdd.HHRDD;
import com.talentica.hungryHippos.rdd.HHRDDInfo;

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
  private HHJavaRDDBuilder hhJavaRDDBuilder;

  public HHSparkSession(SparkContext sc, HHRDD hhRdd, HHRDDInfo hhrddInfo) {
    super(sc);
    this.hhJavaRDDBuilder = HHDataframeFactory.createHHJavaRDD(hhRdd, hhrddInfo, this);
  }

  @Override
  public Dataset<Row> sql(String sqlText) {
    return super.sql(this.sqlText = sqlText);
  }


  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row.
   * 
   * @param beanClazz
   * @return {@code Dataset<Row>}
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public <T> Dataset<Row> mapToDataset(Class<T> beanClazz)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    JavaRDD<T> rddDataframe = hhJavaRDDBuilder.mapToJavaRDD(beanClazz);
    Dataset<Row> dataset =
        sqlContext().createDataFrame(rddDataframe, Class.forName(beanClazz.getName()));
    return dataset;
  }


  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row. It is supported for schema definition of {@code StructType}.
   * 
   * @param fieldName : It is user defined column name. i.e new String[] {"Col1", "Col2", "Col3",
   *        "Col4", "Col5", "Col6", "Col7", "Col8", "Col9"}
   * @return {@code Dataset<Row>}
   * @throws ClassNotFoundException
   * @throws UnsupportedDataTypeException
   */
  public Dataset<Row> mapToDataset(String[] fieldName)
      throws ClassNotFoundException, UnsupportedDataTypeException {
    StructType schema = hhJavaRDDBuilder.getOrCreateSchema(fieldName);
    JavaRDD<Row> rowRDD = hhJavaRDDBuilder.mapToJavaRDD();
    Dataset<Row> dataset = sqlContext().createDataFrame(rowRDD, schema);
    return dataset;
  }

  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row. It is supported for schema definition of {@code StructType}.
   * 
   * @param schema
   * @return {@code Dataset<Row>}
   * @throws ClassNotFoundException
   * @throws UnsupportedDataTypeException
   */
  public Dataset<Row> mapToDataset(StructType schema)
      throws ClassNotFoundException, UnsupportedDataTypeException {
    JavaRDD<Row> rowRDD = hhJavaRDDBuilder.mapToJavaRDD();
    return sqlContext().createDataFrame(rowRDD, schema);
  }


  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row for each partition.
   * 
   * @param beanClazz
   * @return {@code Dataset<Row>}
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public <T> Dataset<Row> mapPartitionToDataset(Class<T> beanClazz)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    JavaRDD<T> rddDataframe = hhJavaRDDBuilder.mapPartitionToJavaRDD(beanClazz);
    Dataset<Row> dataset =
        sqlContext().createDataFrame(rddDataframe, Class.forName(beanClazz.getName()));
    return dataset;
  }

  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row for each partition. It is supported for schema definition of {@code StructType}.
   * 
   * @param fieldName : It is user defined column name. i.e new String[] {"Col1", "Col2", "Col3",
   *        "Col4", "Col5", "Col6", "Col7", "Col8", "Col9"}
   * @return {@code Dataset<Row>}
   * @throws ClassNotFoundException
   * @throws UnsupportedDataTypeException
   */
  public Dataset<Row> mapPartitionToDataset(String[] fieldName)
      throws ClassNotFoundException, UnsupportedDataTypeException {
    StructType schema = hhJavaRDDBuilder.getOrCreateSchema(fieldName);
    JavaRDD<Row> rowRDD = hhJavaRDDBuilder.mapPartitionToJavaRDD();
    return sqlContext().createDataFrame(rowRDD, schema);
  }

  /**
   * To translate the HungryHippos's row representation to required Spark data set format processing
   * row by row for each partition. It is supported for schema definition of {@code StructType}.
   * 
   * @param schema
   * @return {@code Dataset<Row>}
   * @throws ClassNotFoundException
   * @throws UnsupportedDataTypeException
   */
  public Dataset<Row> mapPartitionToDataset(StructType schema)
      throws ClassNotFoundException, UnsupportedDataTypeException {
    JavaRDD<Row> rowRDD = hhJavaRDDBuilder.mapPartitionToJavaRDD();
    return sqlContext().createDataFrame(rowRDD, schema);
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
