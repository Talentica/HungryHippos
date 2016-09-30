package com.talentica.hungryHippos.storage;

import java.io.File;
import java.util.Iterator;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

public class FileStoreAccess implements StoreAccess {

  private int primaryDimension;
  private int numFiles;
  private String hungryHippoFilePath;
  private String base;
  private DataDescription dataDescription;
  private int currentFileId = -1;

  public FileStoreAccess(String hungryHippoFilePath, String base, int keyId,
      int numFiles, DataDescription dataDescription) {
    this.hungryHippoFilePath = hungryHippoFilePath;
    this.primaryDimension = keyId;
    this.numFiles = numFiles;
    this.base = base;
    this.dataDescription = dataDescription;
  }

  @Override
  public Iterator<DataFileAccess> iterator() {

    return new Iterator<DataFileAccess>() {

      @Override
      public boolean hasNext() {
        return getNextFileId() != -1;
      }

      private int getNextFileId() {
        for (int i = currentFileId; i < numFiles; i++) {
          if ((primaryDimension & i) > 0) {
            return i;
          }
        }
        return -1;
      }

      @Override
      public DataFileAccess next() {
        if (hasNext()) {
          currentFileId = getNextFileId();
          File dataFilesDirectory = new File(FileSystemContext.getRootDirectory()
              + hungryHippoFilePath + File.separator + base + currentFileId);
          return new DataFileAccess(dataDescription, dataFilesDirectory);
        }
        return null;
      }
    };
  }

}
