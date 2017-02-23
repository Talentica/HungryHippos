package com.talentica.hungryHippos.storage.util;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;

/**
 * Utility class to read data files generated on nodes after sharding process is completed for
 * input.
 * 
 * @author nitink
 */
public class BinaryFileReader {

  private static Logger LOGGER = LoggerFactory.getLogger(NodeDataFileReader.class);

  private static FieldTypeArrayDataDescription dataDescription;
  private static ShardingApplicationContext context;

  public static void main(String[] args) throws IOException, ClassNotFoundException,
      KeeperException, InterruptedException, JAXBException {
    if (args.length != 2) {
      System.out.println("Argument required sharding folder and data file name");
      System.exit(0);
    }
    context = new ShardingApplicationContext(args[0]);
    String dataFileName = args[1];

    FileInputStream fileInputStream = new FileInputStream(new File(dataFileName));
    DataInputStream dataInputStream = new DataInputStream(fileInputStream);
    File readableDataFile = new File(dataFileName + "_read");
    FileWriter fileWriter = new FileWriter(readableDataFile);
    try {
      DynamicMarshal dynamicMarshal = getDynamicMarshal();
      int noOfBytesInOneDataSet = dataDescription.getSize();
      while (dataInputStream.available() > 0) {
        byte[] bytes = new byte[noOfBytesInOneDataSet];
        dataInputStream.readFully(bytes);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        for (int index = 0; index < dataDescription.getNumberOfDataFields(); index++) {
          Object readableData = dynamicMarshal.readValue(index, buffer);
          if (index != 0) {
            fileWriter.write(",");
          }
          fileWriter.write(readableData.toString());
        }
        fileWriter.write("\n");
      }
    } finally {
      fileWriter.flush();
      fileWriter.close();
      fileInputStream.close();
    }
    LOGGER.info("Output readable data file is written to: " + readableDataFile.getAbsolutePath());
  }


  private static DynamicMarshal getDynamicMarshal() throws ClassNotFoundException,
      FileNotFoundException, KeeperException, InterruptedException, IOException, JAXBException {
    dataDescription = context.getConfiguredDataDescription();
    dataDescription.setKeyOrder(context.getShardingDimensions());
    DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
    return dynamicMarshal;
  }

}
