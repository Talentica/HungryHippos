package com.talentica.hungryhippos.filesystem.context;

import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.filesystem.FileSystemConfig;


/**
 * This class is for handling the filesystem configuration Created by rajkishoreh on 4/7/16.
 */
public class FileSystemContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemContext.class);
  private static HungryHippoCurator curator = HungryHippoCurator.getInstance();

  private static FileSystemConfig fileSystemConfig;

  /**
   * This method uploads the filesystem configuration file in zookeeper
   * 
   * @param fileSystemConfigFile
   * @throws IOException
   * @throws JAXBException
   * @throws InterruptedException
   */
  public static void uploadFileSystemConfig(String fileSystemConfigFile)
      throws IOException, JAXBException, InterruptedException, HungryHippoException {

    LOGGER.info("Updating filesystem configuration on zookeeper");
    String fileSystemConfigurationFile =
        CoordinationConfigUtil.getConfigPath()
            + HungryHippoCurator.ZK_PATH_SEPERATOR + CoordinationConfigUtil.FILE_SYSTEM_CONFIGURATION;
    curator.createPersistentNode(fileSystemConfigurationFile,
        JaxbUtil.unmarshalFromFile(fileSystemConfigFile, FileSystemConfig.class));

  }

  /**
   * This method gets the filesystem configuration object from zookeeper filesystem configuration
   * file
   *
   * @return
   * @throws IOException
   * @throws JAXBException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   * @throws KeeperException
   */
  private static void getFileSystemConfig() {
    try {
      if (fileSystemConfig == null) {

        String fileSystemConfigurationFile =
            CoordinationConfigUtil.getConfigPath() + "/"
                + CoordinationConfigUtil.FILE_SYSTEM_CONFIGURATION;
        fileSystemConfig = (FileSystemConfig) curator.readObject(fileSystemConfigurationFile);
      }
    } catch (Exception e) {
      throw new RuntimeException(e.toString());
    }
  }

  public static String getRootDirectory() {
    getFileSystemConfig();
    return fileSystemConfig.getRootDirectory();
  }

  public static String getDataFilePrefix() {
    getFileSystemConfig();
    return fileSystemConfig.getDataFilePrefix();
  }

  public static boolean isAppendToDataFile() {
    getFileSystemConfig();
    return fileSystemConfig.isAppendToDataFile();
  }

  public static int getServerPort() {
    getFileSystemConfig();
    return fileSystemConfig.getServerPort();
  }

  public static long getQueryRetryInterval() {
    getFileSystemConfig();
    return fileSystemConfig.getQueryRetryInterval();
  }

  public static int getMaxQueryAttempts() {
    getFileSystemConfig();
    return fileSystemConfig.getMaxQueryAttempts();
  }

  public static int getFileStreamBufferSize() {
    getFileSystemConfig();
    return fileSystemConfig.getFileStreamBufferSize();
  }

  public static int getMaxClientRequests() {
    getFileSystemConfig();
    return fileSystemConfig.getMaxClientRequests();
  }

}
