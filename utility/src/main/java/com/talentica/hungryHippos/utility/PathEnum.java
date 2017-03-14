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
  NAMESPACE("NAMESPACE"), CONFIGPATH("CONFIGPATH"), FILESYSTEM(
      "FILESYSTEM"), HOSTS("HOSTS");

  private String pathName;

  private PathEnum(String pathName) {
    this.pathName = pathName;
  }

  public String getPathName() {
    return pathName;
  }
}
