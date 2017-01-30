package com.talentica.hungryHippos.coordination.utility.marshaling;

import java.io.*;
import java.util.Iterator;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.utility.MemoryStatus;

/**
 * {@code FileReader} used for reading file with specified {@link DataParser}.
 * 
 * @author debasishc
 * @since 22/6/15.
 */
public class FileReader implements Reader {

  private InputStream dataInputStream;
  private DataParser dataParser = null;
  private Iterator<DataTypes[]> iterator = null;
  private String filepath = null;

  /**
   * create a new instance of FileReader using the {@value filPath} and {@value parser}.
   * 
   * @param filePath
   * @param parser
   * @throws RuntimeException
   * @throws FileNotFoundException
   */
  public FileReader(String filePath, DataParser parser)
      throws RuntimeException, FileNotFoundException {
    this.dataParser = parser;
    this.filepath = filePath;
    if(MemoryStatus.getUsableMemory()>10485760){
      dataInputStream = new BufferedInputStream(new FileInputStream(filePath),10485760);
    }else if(MemoryStatus.getUsableMemory()>10240){
      dataInputStream = new BufferedInputStream(new FileInputStream(filePath),10240);
    }else{
      dataInputStream = new BufferedInputStream(new FileInputStream(filePath),2048);
    }
    iterator = dataParser.iterator(dataInputStream);
  }

  /**
   * create a new instance of FileReader using the {@value file} and {@value preProcessor}.
   * 
   * @param file
   * @param preProcessor
   * @throws IOException
   * @throws InvalidRowException
   */
  public FileReader(File file, DataParser preProcessor) throws IOException, InvalidRowException {
    this(file.getAbsolutePath(), preProcessor);
  }

  @Override
  public DataTypes[] read() throws RuntimeException {
    if (iterator.hasNext()) {
      return iterator.next();
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    if (dataInputStream != null) {
      dataInputStream.close();
    }
  }

  @Override
  public void reset() throws IOException {
    if (dataInputStream != null) {
      dataInputStream.close();

    }
    if(MemoryStatus.getUsableMemory()>10485760){
      dataInputStream = new BufferedInputStream(new FileInputStream(this.filepath),10485760);
    }else if(MemoryStatus.getUsableMemory()>10240){
      dataInputStream = new BufferedInputStream(new FileInputStream(this.filepath),10240);
    }else{
      dataInputStream = new BufferedInputStream(new FileInputStream(this.filepath),2048);
    }
    iterator = dataParser.iterator(dataInputStream);
  }

}
