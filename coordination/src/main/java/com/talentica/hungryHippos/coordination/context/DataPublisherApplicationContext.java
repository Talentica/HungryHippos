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
package com.talentica.hungryHippos.coordination.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryhippos.config.datapublisher.DatapublisherConfig;

/**
 * {@code DataPublisherApplicationContext} is used for retrieving data publisher configuration
 * file.And its properties.
 * 
 */
public class DataPublisherApplicationContext {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DataPublisherApplicationContext.class);
  private static DatapublisherConfig datapublisherConfig;

  /**
   * retrieves dataPublisher configuration from zookeeper.
   * 
   * @return an instance of {@link DatapublisherConfig}
   */
  public static DatapublisherConfig getDataPublisherConfig() {
    if (datapublisherConfig == null) {
      String configurationFile =
          CoordinationConfigUtil.getConfigPath()
              + HungryHippoCurator.ZK_PATH_SEPERATOR
              + CoordinationConfigUtil.DATA_PUBLISHER_CONFIGURATION;
      try {
        datapublisherConfig =
            (DatapublisherConfig) HungryHippoCurator.getInstance().readObject(configurationFile);
      } catch (HungryHippoException e) {
        LOGGER.error(e.getMessage());
      }
    }
    return datapublisherConfig;
  }

  /**
   * retrieves number of attempts to be made to connect to a node/machine.
   * 
   * @return an int.
   */
  public static int getNoOfAttemptsToConnectToNode() {
    if (datapublisherConfig == null) {
      datapublisherConfig = getDataPublisherConfig();
    }
    return datapublisherConfig.getNoOfAttemptsToConnectToNode();
  }

  /**
   * retieves number of connection retry to be made if server is down. the time mentioned is in
   * millisecond.
   * 
   * @return an int
   */
  public static int getServersConnectRetryIntervalInMs() {
    if (datapublisherConfig == null) {
      datapublisherConfig = getDataPublisherConfig();
    }
    return datapublisherConfig.getServersConnectRetryIntervalInMs();
  }

}
