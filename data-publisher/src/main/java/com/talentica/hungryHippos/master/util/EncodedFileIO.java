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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.coordination.utility.marshaling.Reader;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;

import javax.xml.bind.JAXBException;

public class EncodedFileIO {

  private static ShardingApplicationContext context;
  private static OutputStream outputFile;
  
  public static void main(String[] args) throws JAXBException, FileNotFoundException {
    String shardingFolderPath = args[0];
    String inputFileName = args[1];
    String outputFileName = args[2];
    
    setContext(shardingFolderPath);
    
    try{
      outputFile = new FileOutputStream(outputFileName);
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
          inputFileName, dataParser);
      
      while (true) {
        DataTypes[] parts = null;
        try {
          parts = input.read();
        } catch (InvalidRowException e) {
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
    }
    
  }
  
  private static ShardingApplicationContext setContext(String shardingFolderPath) throws JAXBException, FileNotFoundException {
    context = new ShardingApplicationContext(shardingFolderPath);
    return context;
  }
}
