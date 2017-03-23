/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
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
/**
 * 
 */
package com.talentica.hungryHippos.coordination.context;

import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.coordination.property.Property;
import com.talentica.hungryHippos.coordination.utility.CoordinationProperty;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;

/**
 * {@code CoordinationConfigUtil} is used for uploading and downloading configuration file.
 * 
 * @author pooshans
 *
 */
public class CoordinationConfigUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(CoordinationConfigUtil.class);
  private static Property<CoordinationProperty> property;
  private static String localClusterConfigFilePath;
  private static ClusterConfig clusterConfig;
  public static final String COORDINATION_CONFIGURATION = "coordination-configuration";
  public static final String CLUSTER_CONFIGURATION = "cluster-configuration";
  public static final String CLIENT_CONFIGURATION = "client-configuration";
  public static final String JOB_RUNNER_CONFIGURATION = "job-runner-configuration";
  public static final String DATA_PUBLISHER_CONFIGURATION = "datapublisher-configuration";
  public static final String FILE_SYSTEM_CONFIGURATION = "file-system";
  private static final String ZK_PATH_SEPERATOR = "/";
  private static HungryHippoCurator curator;

  /**
   * Used for reading the location where configuration file has to be saved.
   * 
   * @return an instance of Property<CoordinationProperty>.
   */
  public static Property<CoordinationProperty> getProperty() {
    if (property == null) {
      property = new CoordinationProperty("config-path.properties");
    }
    return property;
  }

  /**
   * Used for uploading configuration details on zookeeper.
   * 
   * @param nodeName
   * @param configurationFile
   * @throws IOException
   * @throws JAXBException
   * @throws InterruptedException
   * @throws HungryHippoException
   */
  public static void uploadConfigurationOnZk(String nodeName, Object configurationFile)
      throws IOException, JAXBException, InterruptedException, HungryHippoException {
    LOGGER.info("uploading  {}  on zookeeper", nodeName);
    if (curator == null) {
      curator = HungryHippoCurator.getInstance();
    }
    curator.createPersistentNode(nodeName, configurationFile);
    LOGGER.info("uploaded  {}  on zookeeper succesfully", nodeName);
  }

  /**
   * sets the {@value localClusterConfigFilePath}.
   * 
   * @param localClusterConfigFilePath
   */
  public static void setLocalClusterConfigPath(String localClusterConfigFilePath) {
    CoordinationConfigUtil.localClusterConfigFilePath = localClusterConfigFilePath;
  }

  /**
   * Used for parsing cluster-config.xml.
   * 
   * @return an instance of {@link ClusterConfig}
   */
  public static ClusterConfig getLocalClusterConfig() {
    ClusterConfig clusterConfig = null;
    try {
      clusterConfig = JaxbUtil.unmarshalFromFile(localClusterConfigFilePath, ClusterConfig.class);
    } catch (FileNotFoundException | JAXBException e) {
      LOGGER.info("Please provide the cluster configuration file path");
      throw new RuntimeException();
    }
    return clusterConfig;
  }

  /**
   * Retrieves cluster configuration from zookeeper.
   * 
   * @return an instance of {@link ClusterConfig}
   */
  public static ClusterConfig getZkClusterConfigCache() {
    if (clusterConfig != null) {
      return clusterConfig;
    }

    String configurationFile = getConfigPath()
            + ZK_PATH_SEPERATOR + CoordinationConfigUtil.CLUSTER_CONFIGURATION;
    try {
      if (curator == null) {
        curator = HungryHippoCurator.getInstance();
      }
      clusterConfig = (ClusterConfig) curator.readObject(configurationFile);
    } catch (HungryHippoException e) {
      LOGGER.error(e.getMessage());
    }

    return clusterConfig;

  }
  
  public static String getFileSystemPath(){
    return getProperty().getValueByKey("zookeeper.filesystem.path");
  }
  
  public static String getConfigPath(){
    return getProperty().getValueByKey("zookeeper.config.path");
  }
  
  public static String getNamespace(){
    return getProperty().getValueByKey("zookeeper.namespace");
  }
  
  public static String getHostsPath(){
    return getProperty().getValueByKey("zookeeper.hosts.path");
  }

}
