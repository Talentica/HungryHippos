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

import com.talentica.hungryhippos.filesystem.{FileStatistics, SerializableComparator}
import org.apache.spark.sql.sources._

/**
  * Created by rajkishoreh.
  */
object HHFileFilterUtility {

  def checkIfFileMeetsCriteria(filter: Filter, fileStatistics: FileStatistics, columnNameToIdxMap: util.HashMap[String, Integer], serializableComparators: util.ArrayList[SerializableComparator[_]]): Boolean = filter match {

    case equalToFilter: EqualTo =>
      fileStatistics.equalTo(columnNameToIdxMap.get(equalToFilter.attribute), equalToFilter.value, serializableComparators)

    case greaterThanFilter: GreaterThan =>
      fileStatistics.greaterThan(columnNameToIdxMap.get(greaterThanFilter.attribute), greaterThanFilter.value, serializableComparators)

    case greaterThanOrEqualFilter: GreaterThanOrEqual =>
      fileStatistics.greaterThanEqualTo(columnNameToIdxMap.get(greaterThanOrEqualFilter.attribute), greaterThanOrEqualFilter.value, serializableComparators)

    case lessThanFilter: LessThan =>
      fileStatistics.lesserThan(columnNameToIdxMap.get(lessThanFilter.attribute), lessThanFilter.value, serializableComparators)

    case lessThanOrEqualFilter: LessThanOrEqual =>
      fileStatistics.lesserThanEqualTo(columnNameToIdxMap.get(lessThanOrEqualFilter.attribute), lessThanOrEqualFilter.value, serializableComparators)

    case inFilter: In => {
      val column = inFilter.attribute
      val values = inFilter.values
      for (value <- values) {
        if (fileStatistics.equalTo(columnNameToIdxMap.get(column), value, serializableComparators))
          return true
      }
      false
    }

    case orFilter: Or =>
      checkIfFileMeetsCriteria(orFilter.left, fileStatistics, columnNameToIdxMap, serializableComparators) || checkIfFileMeetsCriteria(orFilter.right, fileStatistics, columnNameToIdxMap, serializableComparators)

    case andFilter: And =>
      checkIfFileMeetsCriteria(andFilter.left, fileStatistics, columnNameToIdxMap, serializableComparators) && checkIfFileMeetsCriteria(andFilter.right, fileStatistics, columnNameToIdxMap, serializableComparators)

    case notFilter: Not =>
      checkIfFileDoesNotMeetCriteria(notFilter.child, fileStatistics, columnNameToIdxMap, serializableComparators)

    case _: Any => true
  }

  private def checkIfFileDoesNotMeetCriteria(filter: Filter, fileStatistics: FileStatistics, columnNameToIdxMap: util.HashMap[String, Integer], serializableComparators: util.ArrayList[SerializableComparator[_]]): Boolean = filter match {

    case andFilter: And =>
      checkIfFileDoesNotMeetCriteria(andFilter.left, fileStatistics, columnNameToIdxMap, serializableComparators) || checkIfFileDoesNotMeetCriteria(andFilter.right, fileStatistics, columnNameToIdxMap, serializableComparators)

    case orFilter: Or =>
      checkIfFileDoesNotMeetCriteria(orFilter.left, fileStatistics, columnNameToIdxMap, serializableComparators) && checkIfFileDoesNotMeetCriteria(orFilter.right, fileStatistics, columnNameToIdxMap, serializableComparators)

    case notFilter: Not =>
      checkIfFileMeetsCriteria(notFilter.child, fileStatistics, columnNameToIdxMap, serializableComparators)

    case equalToFilter: EqualTo =>
      fileStatistics.notEqualTo(columnNameToIdxMap.get(equalToFilter.attribute), equalToFilter.value, serializableComparators)

    case greaterThanFilter: GreaterThan =>
      fileStatistics.lesserThanEqualTo(columnNameToIdxMap.get(greaterThanFilter.attribute), greaterThanFilter.value, serializableComparators)

    case greaterThanOrEqualFilter: GreaterThanOrEqual =>
      fileStatistics.lesserThan(columnNameToIdxMap.get(greaterThanOrEqualFilter.attribute), greaterThanOrEqualFilter.value, serializableComparators)

    case lessThanFilter: LessThan =>
      fileStatistics.greaterThanEqualTo(columnNameToIdxMap.get(lessThanFilter.attribute), lessThanFilter.value, serializableComparators)

    case lessThanOrEqualFilter: LessThanOrEqual =>
      fileStatistics.greaterThan(columnNameToIdxMap.get(lessThanOrEqualFilter.attribute), lessThanOrEqualFilter.value, serializableComparators)

    case inFilter: In => {
      val column = inFilter.attribute
      val values = inFilter.values
      for (value <- values) {
        if (!fileStatistics.notEqualTo(columnNameToIdxMap.get(column), value, serializableComparators))
          return false
      }
      true
    }

    case _: Any => true
  }

}
