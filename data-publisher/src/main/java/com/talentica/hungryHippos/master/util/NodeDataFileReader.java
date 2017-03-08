package com.talentica.hungryHippos.master.util;

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
import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

/**
 * Utility class to read data files generated on nodes after sharding process is completed for
 * input.
 * 
 * @author nitink
 */
public class NodeDataFileReader {

  private static Logger LOGGER = LoggerFactory.getLogger(NodeDataFileReader.class);

  private static FieldTypeArrayDataDescription dataDescription;
  private static ShardingApplicationContext context;

  public static void main(String[] args) throws IOException, ClassNotFoundException,
      KeeperException, InterruptedException, JAXBException {
    if (args.length != 3) {
      System.out.println(
          "Usage pattern: java -jar <jar name> <path to parent folder of data folder> <client-config-file-path> <sharding-table-folder-path> e.g. java -jar storage.jar ~/home/ ./client-config.xml ./sharding");
      System.exit(0);
    }
    String clientConfigFilePath = args[1];
    ClientConfig clientConfig =
        JaxbUtil.unmarshalFromFile(clientConfigFilePath, ClientConfig.class);
    int sessionTimeOut = Integer.valueOf(clientConfig.getSessionTimout());
    String connectString = clientConfig.getCoordinationServers().getServers();
    HungryHippoCurator.getInstance(connectString, sessionTimeOut);
    context = new ShardingApplicationContext(args[2]);
    int noOfKeys = context.getShardingDimensions().length;
    for (int i = 0; i < 1 << noOfKeys; i++) {
      String dataFilesFolder =
          args[0] + File.separatorChar + FileSystemContext.getDataFilePrefix() + i;
      File[] files = new File(dataFilesFolder).listFiles();
      if(files!=null) {
        for (File encodedFile : files) {
          FileInputStream fileInputStream = new FileInputStream(encodedFile);
          DataInputStream dataInputStream = new DataInputStream(fileInputStream);
          File readableDataFile = new File(encodedFile.getAbsolutePath() + "_read");
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
        }
      }else{
        throw new RuntimeException(dataFilesFolder+" not a valid path");
      }
      LOGGER.info("Output readable data file is written in: " + dataFilesFolder);
    }
  }

  private static DynamicMarshal getDynamicMarshal() throws ClassNotFoundException,
      FileNotFoundException, KeeperException, InterruptedException, IOException, JAXBException {
    dataDescription = context.getConfiguredDataDescription();
    dataDescription.setKeyOrder(context.getShardingDimensions());
    DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
    return dynamicMarshal;
  }

}
