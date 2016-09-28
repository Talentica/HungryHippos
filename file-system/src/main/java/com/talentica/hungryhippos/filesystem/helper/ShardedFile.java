package com.talentica.hungryhippos.filesystem.helper;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.sharding.Column;
import com.talentica.hungryhippos.config.sharding.ShardingClientConfig;

public class ShardedFile {

  public static void read(String filePath, String shardingClientConfigLoc, int numberOfLines)
      throws IOException, JAXBException {
    File file = new File(filePath);
    ShardingClientConfig shardedConfig =
        JaxbUtil.unmarshalFromFile(shardingClientConfigLoc, ShardingClientConfig.class);
    List<Column> columns = shardedConfig.getInput().getDataDescription().getColumn();
    String[] dataTypeDescription = new String[columns.size()];
    FieldTypeArrayDataDescription dataDescription = null;
    for (int index = 0; index < columns.size(); index++) {
      String element = columns.get(index).getDataType() + "-" + columns.get(index).getSize();
      dataTypeDescription[index] = element;
    }
    dataDescription = FieldTypeArrayDataDescription.createDataDescription(dataTypeDescription,
        shardedConfig.getMaximumSizeOfSingleBlockData());
    DynamicMarshal dm = new DynamicMarshal(dataDescription);
    FileInputStream fis = new FileInputStream(file);
    DataInputStream dis = new DataInputStream(fis);
    BinaryFileBuffer bf = new BinaryFileBuffer(dis, dataDescription.getSize());
    int line = 0;
    ByteBuffer byteBuf;
    while (line <= numberOfLines) {
      byteBuf = bf.pop();

      for (int i = 0; i < dataDescription.getNumberOfDataFields(); i++) {
        Object readableData = dm.readValue(i, byteBuf);
        System.out.print(readableData);
        if (i != 0 && i != dataDescription.getNumberOfDataFields() - 1) {
          System.out.print(",");
        }
      }
      System.out.println();
      line++;
    }
  }

}
