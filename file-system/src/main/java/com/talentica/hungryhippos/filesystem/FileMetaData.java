package com.talentica.hungryhippos.filesystem;

import java.io.Serializable;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

public class FileMetaData implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 12356677L;
  @Nonnull
  private String fileName;
  @Nonnull
  private String type;
  @Nonnegative
  private long size;
  private long modificationTime;
  private long creationTime;
  private boolean isDir;


  /**
   * FileMetaData Constructor
   * 
   * @param fileName :- name of the file
   * @param type :- file-type. i.e; output or input .
   * @param size :- file size in bytes.
   */
  public FileMetaData(String fileName, String type, long size, long creationTime,
      long modificationTime, boolean isDir) {
    this.fileName = fileName;
    this.type = type;
    this.size = size;
    this.creationTime = creationTime;
    this.modificationTime = modificationTime;
    this.isDir = isDir;
  }

  public FileMetaData(String metaData) {
    if (!(metaData.startsWith("{") && metaData.endsWith("}"))) {
      throw new RuntimeException("invalid metaData file");
    }
    String[] details = metaData.replace("{", "").replace("}", "").trim().split(",");

    for (String detail : details) {
      String[] keyValue = detail.split(":");
      if ("filename".equals(keyValue[0])) {
        this.fileName = keyValue[1];
      } else if ("type".equals(keyValue[0])) {
        this.type = keyValue[1];
      } else {
        this.size = Long.valueOf(keyValue[1]);
      }
    }

  }

  /**
   * Method returns the name of the file associated with the object.
   * 
   * @return fileName
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * Method returns what type of file i.e; .txt or .csv
   * 
   * @return type
   */
  public String getType() {
    return type;
  }

  /**
   * Methods retrieves the size of the file.
   * 
   * @return
   */
  public long getSize() {
    return size;
  }

  /**
   * Methods set the size of the file.
   * 
   */
  public void setSize(long size) {
    this.size = size;

  }


  public void setIsDir(boolean flag) {
    this.isDir = flag;
  }

  public boolean getIsDir() {
    return this.isDir;
  }

  public void setModificationTime(long modificationTime) {
    this.modificationTime = modificationTime;
  }

  public long getModificationTime() {
    return this.modificationTime;
  }

  public void setCreationTime(long creationTime) {
    this.creationTime = creationTime;
  }

  public long getCreationTime() {
    return this.creationTime;
  }


  @Override
  public String toString() {
    String comma = ",";
    return "{ " + "name:" + this.fileName + comma + "type:" + this.type + comma + "size:"
        + this.size + " }";
  }

}
