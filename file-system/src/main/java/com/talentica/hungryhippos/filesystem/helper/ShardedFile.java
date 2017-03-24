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
package com.talentica.hungryhippos.filesystem.helper;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.sharding.Column;
import com.talentica.hungryhippos.config.sharding.ShardingClientConfig;

/**
 * {@code ShardedFile} method used for eading sharded file.
 * @author sudarshans
 *
 */
public class ShardedFile {

  /**
   * reads the sharded file.
   * @param filePath
   * @param shardingClientConfigLoc
   * @param numberOfLines
   * @throws IOException
   * @throws JAXBException
   */
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
    long size = Files.size(Paths.get(filePath));
    int lineSize = dataDescription.getSize();
    FileInputStream fis = new FileInputStream(file);
    DataInputStream dis = new DataInputStream(fis);
    BinaryFileBuffer bf = new BinaryFileBuffer(dis, lineSize);
    int line = 0;
    ByteBuffer byteBuf;
    while (line <= numberOfLines && size > 0) {
      byteBuf = bf.pop();

      for (int i = 0; i < dataDescription.getNumberOfDataFields(); i++) {
        Object readableData = dm.readValue(i, byteBuf);

        if (i != 0 && i != dataDescription.getNumberOfDataFields()) {
          System.out.print(",");
        }
        System.out.print(readableData);
      }
      System.out.println();
      line++;
      size -= lineSize;

      byteBuf = null;
    }
  }

}
