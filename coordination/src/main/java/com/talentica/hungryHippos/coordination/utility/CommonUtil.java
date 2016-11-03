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
