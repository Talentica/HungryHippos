package com.talentica.hungryHippos.storage;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import com.talentica.hungryHippos.client.domain.DataDescription;

public class DataFileAccess implements Iterable<DataFileAccess> {

  private ByteBuffer byteBuffer = null;

  private byte[] byteBufferBytes;

  private long currentDataFileSize;

  private DataInputStream dataInputStream;

  private int currentFileIndex = -1;

  private File[] dataFiles;

  private DataDescription dataDescription;

  public DataFileAccess(DataDescription dataDescription, File dataFilesDirectory) {
    initialize(dataDescription);
    dataFiles = dataFilesDirectory.listFiles();
  }

  private void initialize(DataDescription dataDescription) {
    this.dataDescription = dataDescription;
    byteBufferBytes = new byte[dataDescription.getSize()];
    byteBuffer = ByteBuffer.wrap(byteBufferBytes);
  }

  private void setCurrentDataFile(File dataFile) {
    try {
      dataInputStream = new DataInputStream(new FileInputStream(dataFile));
      currentDataFileSize = dataFile.length();
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
  }

  private DataFileAccess(File dataFile, DataDescription dataDescription) {
    initialize(dataDescription);
    setCurrentDataFile(dataFile);
  }

  private void closeDatsInputStream(DataInputStream in) throws IOException {
    if (in != null) {
      in.close();
    }
  }

  @Override
  public Iterator<DataFileAccess> iterator() {
    return new Iterator<DataFileAccess>() {

      @Override
      public boolean hasNext() {
        return dataFiles != null && currentFileIndex < dataFiles.length - 1;
      }

      @Override
      public DataFileAccess next() {
        if (hasNext()) {
          currentFileIndex++;
          File dataFile = dataFiles[currentFileIndex];
          setCurrentDataFile(dataFile);
          return new DataFileAccess(dataFile, dataDescription);
        }
        return null;
      }
    };
  }

  public ByteBuffer readNext() {
    try {
      if (isNextReadAvailable()) {
        byteBuffer.clear();
        dataInputStream.readFully(byteBufferBytes);
        currentDataFileSize = currentDataFileSize - byteBufferBytes.length;
        return byteBuffer;
      }
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
    return null;
  }

  public boolean isNextReadAvailable() {
    try {
      if (currentDataFileSize <= 0) {
        closeDatsInputStream(dataInputStream);
        return false;
      }
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
    return currentDataFileSize > 0;
  }

}
