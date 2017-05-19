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
package com.talentica.hungryHippos.node.datareceiver;

import com.talentica.hungryHippos.node.uploaders.AbstractFileUploader;
import com.talentica.hungryHippos.utility.HungryHippoServicesConstants;
import com.talentica.hungryHippos.utility.scp.TarAndUntar;
import com.talentica.hungryhippos.config.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Created by rajkishoreh on 26/12/16.
 */
public class FileUploader extends AbstractFileUploader {

  private String srcFolderPath;
  private Set<String> fileNames;

  public FileUploader(CountDownLatch countDownLatch, String srcFolderPath, String destinationPath,
      int idx,Map<Integer, DataInputStream> dataInputStreamMap, Map<Integer, Socket> socketMap, Node node,
      Set<String> fileNames, String hhFilePath, String tarFilename) {
    super(countDownLatch,srcFolderPath,destinationPath,idx,dataInputStreamMap, socketMap,node,fileNames,hhFilePath,tarFilename);
    this.srcFolderPath = srcFolderPath;
    this.fileNames = fileNames;
  }

  @Override
  protected void createTar(String tarFileName) throws IOException {
    TarAndUntar.createTar(srcFolderPath, fileNames, srcFolderPath + File.separator + tarFileName);
  }

  @Override
  public void writeAppenderType(DataOutputStream dos) throws IOException {
    dos.writeInt(HungryHippoServicesConstants.TAR_DATA_APPENDER);
  }

}
