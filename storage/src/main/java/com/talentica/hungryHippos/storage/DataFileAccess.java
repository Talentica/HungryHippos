package com.talentica.hungryHippos.storage;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

  public DataFileAccess(DataDescription dataDescription, File dataFilesDirectory) {
    try {
      byteBufferBytes = new byte[dataDescription.getSize()];
      byteBuffer = ByteBuffer.wrap(byteBufferBytes);
      initialize(dataFilesDirectory);
      dataFiles = dataFilesDirectory.listFiles();
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
  }

  private void initialize(File dataFile) throws FileNotFoundException {
    dataInputStream = new DataInputStream(new FileInputStream(dataFile));
    currentDataFileSize = dataFile.length();
  }

  private DataFileAccess(File dataFile) {
    try {
      initialize(dataFile);
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
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
        return dataFiles != null && currentFileIndex < dataFiles.length;
      }

      @Override
      public DataFileAccess next() {
        if (hasNext()) {
          currentFileIndex++;
          return new DataFileAccess(dataFiles[currentFileIndex]);
        }
        return null;
      }
    };
  }

  public ByteBuffer readNext() {
    try {
      if (isNextReadAvailable()) {
        byteBuffer.clear();
        byteBuffer.flip();
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
      }
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
    return currentDataFileSize > 0;
  }

}
