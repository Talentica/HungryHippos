/*

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

import java.util

import com.talentica.hungryhippos.datasource.rdd.{HHRDDInfo, HHRDDV2}
import com.talentica.hungryhippos.filesystem.FileStatistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConversions._

class HHRelation(userSchema: StructType, dimension: Integer, hhrddInfo: HHRDDInfo)
                (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with HungryhipposScan
    with Serializable {

  override def schema: StructType = {
    return this.userSchema
  }

  override def sizeInBytes: Long = {
    val totalSize = hhrddInfo.getFileStatisticsMap.map(x => x._2.getDataSize).sum;
    println(hhrddInfo.getDirectoryLocation + " size :" + totalSize)
    totalSize
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[InternalRow] = {
    {

      val schemaFields = userSchema.fields

      val colMap = requiredColumns.map(x => {
        var idx = 0
        var colIdx = 0
        var unmatched = true
        for (col <- schemaFields) {
          if (unmatched && x.equals(col.name)) {
            colIdx = idx
            unmatched = false
          }
          idx += 1
        }
        colIdx
      })
      colMap.foreach(i=>print(" col "+userSchema.fields(i)))
      println("col size : "+colMap.length)
      val fileStatistics = hhrddInfo.getFileStatisticsMap
      val filteredFiles = getFilteredFiles(filters, fileStatistics)
      //val accum = sqlContext.sparkContext.longAccumulator("counter");
      val requiredSchemaFields = new Array[StructField](requiredColumns.length)
      for (i <- requiredSchemaFields.indices) {
        requiredSchemaFields(i) = userSchema.fields(colMap(i))
      }
      val requiredSchema = new StructType(requiredSchemaFields)
      val rdd = new HHRDDV2(sqlContext.sparkContext, hhrddInfo, Array(dimension), filteredFiles, filters, requiredSchema, colMap)
      /*val rows = rdd.map(x => {
        accum.add(1)
        x
      })
      rows*/
      rdd
    }

  }


  def getFilteredFiles(filters: Array[Filter], map: java.util.Map[String, FileStatistics]): java.util.Set[String] = {

    val filteredFiles = new util.HashSet[String]

    val javaItr = map.entrySet().iterator()
    val scalaItr = asScalaIterator(javaItr)

    scalaItr.filter(entry => !entry.getValue().isEmpty()).foreach(
      entry => {
        var isValid = true
        for (filter <- filters) {
          isValid = isValid && HHFileFilterUtility.checkIfFileMeetsCriteria(filter, entry.getValue())
        }
        if (isValid) {
          filteredFiles.add(entry.getKey())
        }
      })

    filteredFiles
  }


}

*/