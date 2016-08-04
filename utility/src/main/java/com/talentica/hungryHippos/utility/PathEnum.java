/**
 * 
 */
package com.talentica.hungryHippos.utility;

/**
 * This enum is used only on the start of the application. Do not modify unless required on start of
 * the application
 * 
 * @author PooshanS
 *
 */
public enum PathEnum {
  NAMESPACE("NAMESPACE"), BASEPATH("BASEPATH"), ALERTPATH("ALERTPATH"), CONFIGPATH("CONFIGPATH"), FILESYSTEM(
      "FILESYSTEM"), SHARDING_TABLE("SHARDING-TABLE"),
  JOB_CONFIG("JOB_CONFIG"),JOB_STATUS("JOB_STATUS"),
  COMPLETED_JOBS("COMPLETED_JOBS"), FAILED_JOBS("FAILED_JOBS"),
  STARTED_JOB_ENTITY("STARTED_JOB_ENTITY"),COMPLETED_JOB_ENTITY("COMPLETED_JOB_ENTITY"),
  PENDING_JOBS("PENDING_JOBS"),IN_PROGRESS_JOBS("IN_PROGRESS_JOBS"),
  COMPLETED_JOB_NODES("COMPLETED_JOB_NODES"),FAILED_JOB_NODES("FAILED_JOB_NODES");

  private String pathName;

  private PathEnum(String pathName) {
    this.pathName = pathName;
  }

  public String getPathName() {
    return pathName;
  }
}
