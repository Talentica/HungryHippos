package com.talentica.hungryHippos.storage;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

public class FileStoreAccess implements StoreAccess {

  private List<RowProcessor> rowProcessors = new ArrayList<>();
  private int keyId;
  private int numFiles;
  private String base;
  private ByteBuffer byteBuffer = null;
  private byte[] byteBufferBytes;
  private String hungryHippoFilePath;

  public FileStoreAccess(String hungryHippoFilePath, String base, int keyId, int numFiles,
      DataDescription dataDescription) {
    this.hungryHippoFilePath = hungryHippoFilePath;
    this.keyId = keyId;
    this.numFiles = numFiles;
    this.base = base;
    byteBufferBytes = new byte[dataDescription.getSize()];
    byteBuffer = ByteBuffer.wrap(byteBufferBytes);
  }

  @Override
  public void addRowProcessor(RowProcessor rowProcessor) {
    rowProcessors.add(rowProcessor);
  }

  @Override
  public void processRows() {
    try {
      int keyIdBit = 1 << keyId;
      for (int i = 0; i < numFiles; i++) {
        if ((keyIdBit & i) > 0) {
          processRows(i);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void processRows(int fileId) throws Exception {
    DataInputStream in = null;
    File dataFilesDirectory = new File(FileSystemContext.getRootDirectory() + hungryHippoFilePath
        + File.separator + base + fileId);
    File[] dataFiles = dataFilesDirectory.listFiles();
    if (dataFiles != null) {
      for (File dataFile : dataFiles) {
        try {
          in = new DataInputStream(new FileInputStream(dataFile));
          long dataFileSize = dataFile.length();
          while (dataFileSize > 0) {
            byteBuffer.clear();
            in.readFully(byteBufferBytes);
            dataFileSize = dataFileSize - byteBufferBytes.length;
            for (RowProcessor p : rowProcessors) {
              p.processRow(byteBuffer);
            }
            byteBuffer.flip();
          }
        } finally {
          closeDatsInputStream(in);
        }
      }
    }
  }

  private void closeDatsInputStream(DataInputStream in) throws IOException {
    if (in != null) {
      in.close();
    }
  }

  public void clear() {
    rowProcessors.clear();
  }

}
