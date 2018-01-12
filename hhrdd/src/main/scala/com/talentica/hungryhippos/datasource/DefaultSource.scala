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

import com.talentica.hungryhippos.datasource.rdd.HHRDDHelper
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
  * Created by rajkishoreh.
  */
class DefaultSource extends RelationProvider with SchemaRelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    parameters.getOrElse("path", sys.error("'path' must be specified for our data."))
    val dimension = parameters.getOrElse("dimension", "0")
    val hhRDDInfo = HHRDDHelper.getHhrddInfo(parameters.get("path").get)
    if (schema != null) {
      new HHPrunedFilteredRelation(schema, dimension.toInt, hhRDDInfo)(sqlContext)
    } else {
      val schema = HHSchemaReader.readSchema(hhRDDInfo.getContext)
      new HHPrunedFilteredRelation(schema, dimension.toInt, hhRDDInfo)(sqlContext);
    }
  }



}
