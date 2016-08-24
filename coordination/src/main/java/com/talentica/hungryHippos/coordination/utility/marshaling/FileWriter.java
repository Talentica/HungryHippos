/**
 * 
 */
package com.talentica.hungryHippos.coordination.utility.marshaling;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.InvalidRowException;

/**
 * @author pooshans
 *
 */
public class FileWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileWriter.class);
  private File fileOut;
  private FileOutputStream fileOutputStream;
  private OutputStreamWriter fileOutputWriter;
  private boolean isFileCreated = false;
  private String fileName;
  
  public FileWriter(String fileName){
    this.fileName = fileName;
  }

  public boolean openFile() {
    fileOut = new File(fileName);
    if (fileOut.exists())
      fileOut.delete();
    try {
      isFileCreated = fileOut.createNewFile();
    } catch (IOException e) {
      LOGGER.error("Unable to create the file due to {}", e.getMessage());
    }
    try {
      fileOutputStream = new FileOutputStream(fileOut, true);
      fileOutputWriter = new OutputStreamWriter(fileOutputStream);
    } catch (FileNotFoundException e) {
      LOGGER.error("Exception {}", e.getMessage());
    }
    return isFileCreated;
  }

  public void write(String data) {
    if (isFileCreated) {
      if (fileOutputWriter == null) {
        throw new RuntimeException("Please use openfile  before write");
      }
      try {
        fileOutputWriter.write(data);
        fileOutputWriter.write("\n");
        fileOutputWriter.flush();
      } catch (IOException e) {
        LOGGER.error("unable to write in the file due to  {}", e.getMessage());
      }
    }
  }

  public void close() {
    if (fileOutputWriter != null)
      try {
        fileOutputWriter.close();
      } catch (IOException e) {
        LOGGER.error("Unable to close the file");
      }
  }

  public void flushData(int lineNo, InvalidRowException e) {
    write("Error in line :: [" + (lineNo) + "]  and columns(true are bad values) :: "
        + Arrays.toString(e.getColumns()) + " and row :: [" + e.getBadRow().toString() + "]");
  }
}
