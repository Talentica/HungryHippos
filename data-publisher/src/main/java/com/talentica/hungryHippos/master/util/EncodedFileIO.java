package com.talentica.hungryHippos.master.util;

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

public class EncodedFileIO {

  private static ShardingApplicationContext context;
  private static OutputStream outputFile;
  
  public static void main(String[] args){
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
      DataParser dataParser =
          (DataParser) Class.forName(dataParserClassName).getConstructor(DataDescription.class)
              .newInstance(context.getConfiguredDataDescription());
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
          Object value = parts[i].clone();
          dynamicMarshal.writeValue(i, value, byteBuffer);
        }
        outputFile.write(buf);
      }
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
  
  private static ShardingApplicationContext setContext(String shardingFolderPath){
    context = new ShardingApplicationContext(shardingFolderPath);
    return context;
  }
}
