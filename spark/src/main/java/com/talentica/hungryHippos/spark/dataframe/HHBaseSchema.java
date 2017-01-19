/**
 * 
 */
package com.talentica.hungryHippos.spark.dataframe;

import java.util.List;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.columnar.BYTE;
import org.apache.spark.sql.execution.columnar.DOUBLE;
import org.apache.spark.sql.execution.columnar.FLOAT;
import org.apache.spark.sql.execution.columnar.INT;
import org.apache.spark.sql.execution.columnar.LONG;
import org.apache.spark.sql.execution.columnar.SHORT;
import org.apache.spark.sql.execution.columnar.STRING;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;

/**
 * @author pooshans
 *
 */
public class HHBaseSchema extends BaseRelation {
  private SQLContext sqlContext;
  private List<HHColumnType> columns;
  private FieldTypeArrayDataDescription dataDescription;

  public HHBaseSchema(SQLContext sqlContext, List<HHColumnType> columns,
      FieldTypeArrayDataDescription dataDescription) {
    this.sqlContext = sqlContext;
  }

  @Override
  public StructType schema() {
    StructField[] structFields = new StructField[columns.size()];
    for (int col = 0; col < columns.size(); col++) {
      DataLocator locator = dataDescription.locateField(col);
      StructField structField;
      switch (locator.getDataType()) {
        case BYTE:
          structField = new StructField(("key" + (col + 1)), BYTE.dataType(), false, null);
          break;
        case CHAR:
          structField = new StructField(("key" + (col + 1)), STRING.dataType(), false, null);
          break;
        case SHORT:
          structField = new StructField(("key" + (col + 1)), SHORT.dataType(), false, null);
          break;
        case INT:
          structField = new StructField(("key" + (col + 1)), INT.dataType(), false, null);
          break;
        case LONG:
          structField = new StructField(("key" + (col + 1)), LONG.dataType(), false, null);
          break;
        case FLOAT:
          structField = new StructField(("key" + (col + 1)), FLOAT.dataType(), false, null);
          break;
        case DOUBLE:
          structField = new StructField(("key" + (col + 1)), DOUBLE.dataType(), false, null);
          break;
        case STRING:
          structField = new StructField(("key" + (col + 1)), STRING.dataType(), false, null);
          break;
        default:
          throw new RuntimeException("Invalid data type");
      }
      structFields[col] = structField;
    }
    return new StructType(structFields);

  }

  @Override
  public SQLContext sqlContext() {
    return this.sqlContext;
  }

}
