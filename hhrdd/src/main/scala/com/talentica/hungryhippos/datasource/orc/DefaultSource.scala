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

package com.talentica.hungryhippos.datasource.orc

import java.util

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable

/**
  * Created by rajkishoreh.
  */
class DefaultSource extends RelationProvider with SchemaRelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    parameters.getOrElse("path", sys.error("'path' must be specified for our data."))
    val dimensionCol = parameters.getOrElse("dimension", "").toLowerCase
    val distributedPath = parameters.get("path").get
    var dataSchema = schema
    var partitionSchema = StructType(new util.ArrayList[StructField]())
    val  hhInMemoryFileIndexHelper = HHOrcHelper.getHHInMemoryFileIndexHelper(sqlContext.sparkSession,
      distributedPath,dimensionCol)
    if (schema == null) {
      dataSchema = hhInMemoryFileIndexHelper.getSchema
    }

    val options= new mutable.HashMap[String,String]().toMap

    val hhInMemoryFileIndex = new HHInMemoryFileIndex(sqlContext.sparkSession,
      options, Some(partitionSchema), hhInMemoryFileIndexHelper,hhInMemoryFileIndexHelper.getDimension)
    val loader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

    val providerClass = loader.loadClass("org.apache.spark.sql.hive.orc.OrcFileFormat")
    val fileInputFormat = providerClass.newInstance().asInstanceOf[FileFormat]

    val caseInsensitiveOptions = CaseInsensitiveMap(options)
    caseInsensitiveOptions.get("path").toSeq ++ hhInMemoryFileIndex.rootPaths
    HadoopFsRelation(hhInMemoryFileIndex,hhInMemoryFileIndex.partitionSchema,dataSchema,None,fileInputFormat,
      caseInsensitiveOptions)(sqlContext.sparkSession)
  }

  def getSparkClassLoader: ClassLoader = getClass.getClassLoader


}
