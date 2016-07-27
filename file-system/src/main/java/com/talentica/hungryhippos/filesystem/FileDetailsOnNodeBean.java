package com.talentica.hungryhippos.filesystem;

import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryhippos.config.coordination.Node;

/**
 * 
 * @author sudarshans Each node has details about FileMetaData or the file details.
 */
public class FileDetailsOnNodeBean extends Node {

  private static List<FileMetaData> fileMetaData = new ArrayList<>();

  public void setFileMetaData(FileMetaData fileDetails) {
    fileMetaData.add(fileDetails);
  }

  public List<FileMetaData> getFileMetaData() {
    return fileMetaData;
  }

  public FileDetailsOnNodeBean() {

  }
}
