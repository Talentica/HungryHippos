/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.coordination.utility;

public class CommonUtil {

  public enum ZKJobNodeEnum {

    PUSH_JOB_NOTIFICATION("PUSH_JOB"), PULL_JOB_NOTIFICATION("PULL_JOB"), START_ROW_COUNT(
        "START_ROW_COUNT"), START_JOB_MATRIX("START_JOB_MATRIX"), FINISH_JOB_MATRIX(
            "FINISH_JOB_MATRIX"), FINISH_ROW_COUNT("FINISH_ROW_COUNT"), DOWNLOAD_FINISHED(
                "DOWNLOAD_FINISHED"), SHARDING_COMPLETED(
                    "SHARDING_COMPLETED"), DATA_PUBLISHING_COMPLETED(
                        "DATA_PUBLISHING_COMPLETED"), START_NODE_FOR_DATA_RECIEVER(
                            "START_NODE_FOR_DATA_RECIEVER"), SAMPLING_COMPLETED(
                                "SAMPLING_COMPLETED"), INPUT_DOWNLOAD_COMPLETED(
                                    "INPUT_DOWNLOAD_COMPLETED"), SHARDING_FAILED(
                                        "SHARDING_FAILED"), DATA_PUBLISHING_FAILED(
                                            "DATA_PUBLISHING_FAILED"), FINISH_JOB_FAILED(
                                                "FINISH_JOB_FAILED"), END_JOB_MATRIX(
                                                    "END_JOB_MATRIX"), ALL_OUTPUT_FILES_DOWNLOADED(
                                                        "ALL_OUTPUT_FILES_DOWNLOADED"), OUTPUT_FILES_ZIPPED_AND_TRANSFERRED(
                                                            "OUTPUT_FILES_ZIPPED_AND_TRANSFERRED"), DROP_DROPLETS(
                                                                "DROP_DROPLETS"), ERROR_ENCOUNTERED(
                                                                    "ERROR_ENCOUNTERED");

    private String jobNode;

    private ZKJobNodeEnum(String jobNode) {
      this.jobNode = jobNode;
    }

    public String getZKJobNode() {
      return this.jobNode;
    }

  }
}
