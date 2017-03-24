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
        CoordinationConfigUtil.getConfigPath() + "/"
            + CoordinationConfigUtil.FILE_SYSTEM_CONFIGURATION;
    String connectString = config.getCoordinationServers().getServers();
    int sessionTimeOut = Integer.parseInt(config.getSessionTimout());
    HungryHippoCurator curator = HungryHippoCurator.getInstance(connectString, sessionTimeOut);
    FileSystemConfig fileSystemConfig = (FileSystemConfig) curator.readObject(fileSystemConfigFile);
    String rootDir = fileSystemConfig.getRootDirectory();

    CleanFileSystem cleanFileSystem = new CleanFileSystem(new NodeFileSystem(rootDir));

    cleanFileSystem.deleteFilesWhichAreNotPartOFZK(File.separatorChar + ROOT_DIR);
  }

  private static void validateArgs(String[] args) {

    if (args.length < 2) {
      throw new IllegalArgumentException("Need client-config.xml location details.");
    }
  }

}
