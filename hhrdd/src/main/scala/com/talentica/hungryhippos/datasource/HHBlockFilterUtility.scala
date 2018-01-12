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

import com.talentica.hungryhippos.filesystem.{BlockStatistics, SerializableComparator}
import org.apache.spark.sql.sources._

import scala.collection.JavaConversions._

/**
  * Created by rajkishoreh.
  */
object HHBlockFilterUtility {

  def getFilteredBlocks(filters: Array[Filter], blockStatisticsList: java.util.List[BlockStatistics]): Iterator[BlockStatistics] = {


    val javaItr = blockStatisticsList.iterator()
    val scalaItr = asScalaIterator(javaItr)
    val scalaFilteredItr = scalaItr.filter(blockStatistics => {
      var isValid: Boolean = true
      for (filter <- filters) {
        // isValid = isValid && checkIfBlockMeetsCriteria(filter, blockStatistics)
      }
      isValid
    })
    scalaFilteredItr
  }

  def getFilteredBlocks(filters: Array[Filter], blockStatisticsList: java.util.List[BlockStatistics], serializableComparators: util.ArrayList[SerializableComparator[_]], columnNameToIdxMap: util.HashMap[String, Integer]): Iterator[BlockStatistics] = {

    val javaItr = blockStatisticsList.iterator()
    val scalaItr = asScalaIterator(javaItr)
    val scalaFilteredItr = scalaItr.filter(blockStatistics => {
      var isValid: Boolean = true
      for (filter <- filters) {
        isValid = isValid && checkIfBlockMeetsCriteria(filter, blockStatistics, columnNameToIdxMap, serializableComparators)
      }
      isValid
    })
    scalaFilteredItr
  }

  private def checkIfBlockMeetsCriteria(filter: Filter, blockStatistics: BlockStatistics, columnNameToIdxMap: util.HashMap[String, Integer], serializableComparators: util.ArrayList[SerializableComparator[_]]): Boolean = filter match {

    case equalToFilter: EqualTo =>
      blockStatistics.equalTo(columnNameToIdxMap.get(equalToFilter.attribute), equalToFilter.value, serializableComparators)

    case greaterThanFilter: GreaterThan =>
      blockStatistics.greaterThan(columnNameToIdxMap.get(greaterThanFilter.attribute), greaterThanFilter.value, serializableComparators)

    case greaterThanOrEqualFilter: GreaterThanOrEqual =>
      blockStatistics.greaterThanEqualTo(columnNameToIdxMap.get(greaterThanOrEqualFilter.attribute), greaterThanOrEqualFilter.value, serializableComparators)

    case lessThanFilter: LessThan =>
      blockStatistics.lesserThan(columnNameToIdxMap.get(lessThanFilter.attribute), lessThanFilter.value, serializableComparators)

    case lessThanOrEqualFilter: LessThanOrEqual =>
      blockStatistics.lesserThanEqualTo(columnNameToIdxMap.get(lessThanOrEqualFilter.attribute), lessThanOrEqualFilter.value, serializableComparators)

    case inFilter: In => {
      val column = inFilter.attribute
      val values = inFilter.values
      for (value <- values) {
        if (blockStatistics.equalTo(columnNameToIdxMap.get(column), value, serializableComparators))
          return true
      }
      false
    }

    case orFilter: Or =>
      checkIfBlockMeetsCriteria(orFilter.left, blockStatistics, columnNameToIdxMap, serializableComparators) || checkIfBlockMeetsCriteria(orFilter.right, blockStatistics, columnNameToIdxMap, serializableComparators)

    case andFilter: And =>
      checkIfBlockMeetsCriteria(andFilter.left, blockStatistics, columnNameToIdxMap, serializableComparators) && checkIfBlockMeetsCriteria(andFilter.right, blockStatistics, columnNameToIdxMap, serializableComparators)

    case notFilter: Not =>
      checkIfBlockDoesNotMeetCriteria(notFilter.child, blockStatistics, columnNameToIdxMap, serializableComparators)

    case _: Any => true
  }

  private def checkIfBlockDoesNotMeetCriteria(filter: Filter, blockStatistics: BlockStatistics, columnNameToIdxMap: util.HashMap[String, Integer], serializableComparators: util.ArrayList[SerializableComparator[_]]): Boolean = filter match {

    case andFilter: And =>
      checkIfBlockDoesNotMeetCriteria(andFilter.left, blockStatistics, columnNameToIdxMap, serializableComparators) || checkIfBlockDoesNotMeetCriteria(andFilter.right, blockStatistics, columnNameToIdxMap, serializableComparators)

    case orFilter: Or =>
      checkIfBlockDoesNotMeetCriteria(orFilter.left, blockStatistics, columnNameToIdxMap, serializableComparators) && checkIfBlockDoesNotMeetCriteria(orFilter.right, blockStatistics, columnNameToIdxMap, serializableComparators)

    case notFilter: Not =>
      checkIfBlockMeetsCriteria(notFilter.child, blockStatistics, columnNameToIdxMap, serializableComparators)

    case equalToFilter: EqualTo =>
      blockStatistics.notEqualTo(columnNameToIdxMap.get(equalToFilter.attribute), equalToFilter.value, serializableComparators)

    case greaterThanFilter: GreaterThan =>
      blockStatistics.lesserThanEqualTo(columnNameToIdxMap.get(greaterThanFilter.attribute), greaterThanFilter.value, serializableComparators)

    case greaterThanOrEqualFilter: GreaterThanOrEqual =>
      blockStatistics.lesserThan(columnNameToIdxMap.get(greaterThanOrEqualFilter.attribute), greaterThanOrEqualFilter.value, serializableComparators)

    case lessThanFilter: LessThan =>
      blockStatistics.greaterThanEqualTo(columnNameToIdxMap.get(lessThanFilter.attribute), lessThanFilter.value, serializableComparators)

    case lessThanOrEqualFilter: LessThanOrEqual =>
      blockStatistics.greaterThan(columnNameToIdxMap.get(lessThanOrEqualFilter.attribute), lessThanOrEqualFilter.value, serializableComparators)

    case inFilter: In => {
      val column = inFilter.attribute
      val values = inFilter.values
      for (value <- values) {
        if (!blockStatistics.notEqualTo(columnNameToIdxMap.get(column), value, serializableComparators))
          return false
      }
      true
    }

    case _: Any => true
  }


}
