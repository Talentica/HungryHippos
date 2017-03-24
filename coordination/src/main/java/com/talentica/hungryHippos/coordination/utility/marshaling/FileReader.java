/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
