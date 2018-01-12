/*
 * *****************************************************************************
 *   Copyright 2017 Talentica Software Pvt. Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *  *****************************************************************************
 */

package com.talentica.hungryhippos.datasource

import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

import scala.collection.JavaConversions

/**
  * Created by rajkishoreh.
  */
object HHSchemaReader {

  def readSchema(shardingFolderPath: String):StructType = {
    val context = new ShardingApplicationContext(shardingFolderPath)
    val columns = context.getShardingClientConfig().getInput().getDataDescription().getColumn()
    val javaItr = columns.iterator()
    val scalaItr = JavaConversions.asScalaIterator(javaItr)
    val structFields = new Array[StructField](columns.size())
    var idx = 0
    while (scalaItr.hasNext) {
      val column = scalaItr.next()
      structFields(idx) = new StructField(column.getName(), getDataType(column.getDataType().toUpperCase()), false)
      idx += 1
    }
    new StructType(structFields)
  }

  def readSchema(context: ShardingApplicationContext):StructType = {
    val columns = context.getShardingClientConfig().getInput().getDataDescription().getColumn()
    val javaItr = columns.iterator()
    val scalaItr = JavaConversions.asScalaIterator(javaItr)
    val structFields = new Array[StructField](columns.size())
    var idx = 0
    while (scalaItr.hasNext) {
      val column = scalaItr.next()
      structFields(idx) = new StructField(column.getName(), getDataType(column.getDataType().toUpperCase()), false)
      idx += 1
    }
    new StructType(structFields)
  }

  private def getDataType(strDataType: String): DataType = strDataType match {
    case "INT" => DataTypes.IntegerType;
    case "LONG" => DataTypes.LongType;
    case "FLOAT" => DataTypes.FloatType;
    case "DATE" => DataTypes.DateType;
    case "TIMESTAMP" => DataTypes.TimestampType;
    case "BYTE" => DataTypes.ByteType;
    case "DOUBLE" => DataTypes.DoubleType;
    case "SHORT" => DataTypes.ShortType;
    case "STRING" => DataTypes.StringType;
    case _ => {
      sys.error("Invalid Data type");
    }
  }
}
