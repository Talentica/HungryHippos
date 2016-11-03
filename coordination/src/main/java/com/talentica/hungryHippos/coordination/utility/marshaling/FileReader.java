package com.talentica.hungryHippos.coordination.utility.marshaling;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.InvalidRowException;

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
    dataInputStream = new FileInputStream(filePath);
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
    dataInputStream = new FileInputStream(this.filepath);
    iterator = dataParser.iterator(dataInputStream);
  }

}
