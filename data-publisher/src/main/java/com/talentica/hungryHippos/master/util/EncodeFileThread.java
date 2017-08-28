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
package com.talentica.hungryHippos.master.util;

import java.io.*;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;

import javax.xml.bind.JAXBException;

public class EncodeFileThread implements Runnable{

  private String shardingFolderPath;
  private String splitFilePath;
  private String outputFileName;
  private ShardingApplicationContext context;
  private OutputStream outputFile;
  private String commonCommandArgs;
  private int currentChunk;
  private static Logger logger = LoggerFactory.getLogger(EncodeFileThread.class);
  
  public EncodeFileThread(String splitFilePath,String shardingFolderPath,String outputFileName,
      String commonCommandArgs, int currentChunk){
    this.splitFilePath = splitFilePath;
    this.shardingFolderPath = shardingFolderPath;
    this.outputFileName = outputFileName;
    this.commonCommandArgs = commonCommandArgs;
    this.currentChunk = currentChunk;
  }
  @Override
  public void run() {

    String splitFileName = splitFilePath + "_" + currentChunk + "_txt";
    File file = new File(splitFileName);
    FileOutputStream fos = null;
    try{
      setContext(shardingFolderPath);
      Process process  = Runtime.getRuntime().exec(commonCommandArgs + " " + currentChunk + " " + splitFileName);
      file.deleteOnExit();
      int processStatus = process.waitFor();
      if (processStatus != 0) {
          String line = "";
          BufferedReader errReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
          while ((line = errReader.readLine()) != null) {
              logger.error(line);
          }
          errReader.close();
          logger.error("[{}] {} File encoding failed for {}",Thread.currentThread().getName(),currentChunk, outputFileName);
          throw new RuntimeException("File encoding failed");
      }
      new File(outputFileName + currentChunk).getParentFile().mkdirs();
      fos= new FileOutputStream(outputFileName + currentChunk);
      outputFile = new BufferedOutputStream(fos,20480000);
      FieldTypeArrayDataDescription dataDescription =
          context.getConfiguredDataDescription();
      dataDescription.setKeyOrder(context.getShardingDimensions());
      byte[] buf = new byte[dataDescription.getSize()];
      ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
      DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
      String dataParserClassName =
          context.getShardingClientConfig().getInput().getDataParserConfig().getClassName();
      String delimiter =
              context.getShardingClientConfig().getInput().getDataParserConfig().getDelimiter();
      DataParser dataParser =
              (DataParser) Class.forName(dataParserClassName).getConstructor(DataDescription.class,char.class)
                      .newInstance(context.getConfiguredDataDescription(),delimiter.charAt(0));
      Reader input = new com.talentica.hungryHippos.coordination.utility.marshaling.FileReader(
          splitFileName, dataParser);
      
      while (true) {
        DataTypes[] parts = null;
        try {
          parts = input.read();
        } catch (InvalidRowException e) {
          e.printStackTrace();
          continue;
        }
        if (parts == null) {
          input.close();
          break;
        }
        for (int i = 0; i < dataDescription.getNumberOfDataFields(); i++) {
          dynamicMarshal.writeValue(i, parts[i], byteBuffer);
        }
        outputFile.write(buf);
      }
      outputFile.flush();
      fos.flush();
      file.delete();
      logger.info(currentChunk +" encoded successfully");
    }catch(Exception e){
      e.printStackTrace();
    }finally{
      if(outputFile != null){
        try {
          outputFile.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if(fos!=null){
        try {
          fos.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
  
  private ShardingApplicationContext setContext(String shardingFolderPath) throws JAXBException, FileNotFoundException {
    context = new ShardingApplicationContext(shardingFolderPath);
    return context;
  }

}
