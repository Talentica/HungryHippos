package com.talentica.hungryhippos.filesystem.main;

import java.io.File;
import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.filesystem.FileSystemConfig;
import com.talentica.hungryhippos.filesystem.CleanFileSystem;
import com.talentica.hungryhippos.filesystem.NodeFileSystem;

/**
 * Class used for cleaning File System.
 * 
 * @author sudarshans
 *
 */
public class CleanFileSystemMain {

  private static final String ROOT_DIR = "HungryHipposFs";
  private static String clientConfig;

  /**
   * 
   * @param args
   * 
   * @throws JAXBException
   * @throws FileNotFoundException
   * 
   * @throws JAXBException
   * @throws FileNotFoundException
   * @throws HungryHippoException
   * 
   */
  public static void main(String[] args)
      throws FileNotFoundException, JAXBException, HungryHippoException {

    validateArgs(args);
    clientConfig = args[0];
    ClientConfig config = JaxbUtil.unmarshalFromFile(clientConfig, ClientConfig.class);
    String fileSystemConfigFile =
        CoordinationConfigUtil.getProperty().getValueByKey("zookeeper.config_path") + "/"
            + CoordinationConfigUtil.FILE_SYSTEM_CONFIGURATION;
    String connectString = config.getCoordinationServers().getServers();
    int sessionTimeOut = Integer.parseInt(config.getSessionTimout());
    HungryHippoCurator curator = HungryHippoCurator.getInstance(connectString, sessionTimeOut);
    FileSystemConfig fileSystemConfig = (FileSystemConfig) curator.readObject(fileSystemConfigFile);
    String rootDir = fileSystemConfig.getRootDirectory();

    CleanFileSystem cleanFileSystem = new CleanFileSystem(new NodeFileSystem(rootDir));

    cleanFileSystem.DeleteFilesWhichAreNotPartOFZK(File.separatorChar + ROOT_DIR);
  }

  private static void validateArgs(String[] args) {

    if (args.length < 2) {
      throw new IllegalArgumentException("Need client-config.xml location details.");
    }
  }

}
