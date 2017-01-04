/**
 * 
 */
package com.talentica.hungryHippos.rdd;


/**
 * @author pooshans
 */
public class CustomHHJobConfiguration {

  private String distributedPath;
  private String outputDirectory;

  public String getDistributedPath() {
    return distributedPath;
  }

  public CustomHHJobConfiguration(String distributedPath, String outputDirectory) {
    this.distributedPath = distributedPath;
    this.outputDirectory = outputDirectory;
  }

  public String getOutputDirectory() {
    return outputDirectory;
  }
}
