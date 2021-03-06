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
package com.talentica.hungryHippos.utility.scp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.jcraft.jsch.JSchException;
import com.talentica.hungryHippos.utility.SecureShellExecutor;

public class Jscp {

  private static final String MAKE_DIRECTORIES_COMMAND = "mkdir -p ";

  private static final Logger LOGGER = Logger.getLogger(Jscp.class);

  private static final List<String> EMPTY_LIST = new ArrayList<>(0);

  public static void scpTarGzippedFile(SecureContext secureContext, String sourceDirectory,
      String remoteDirectory, String gzipFileName) {
    exec(secureContext, sourceDirectory, remoteDirectory, EMPTY_LIST, gzipFileName);
  }

  private static void exec(SecureContext secureContext, String sourceDirectory,
      String remoteDirectory, List<String> processIgnores, String gzipFileName) {
    try {
      makeRemoteDestinationDirectoryStructure(secureContext, remoteDirectory);
      String zipFilePath =
          TarAndGzip.folder(new File(sourceDirectory), processIgnores, gzipFileName);
      String remoteDestinationFilePath = remoteDirectory + "/" + gzipFileName + ".tar.gz";
      Scp.exec(secureContext, zipFilePath, remoteDestinationFilePath);
      LOGGER.info("scp'ing: " + zipFilePath + " " + secureContext.getUsername() + "@"
          + secureContext.getHost() + ":" + remoteDestinationFilePath);
      FileUtils.deleteQuietly(new File(zipFilePath));
    } catch (IOException | JSchException exception) {
      throw new RuntimeException(exception);
    }
  }

  private static void makeRemoteDestinationDirectoryStructure(SecureContext secureContext,
      String remoteDirectory) {
    SecureShellExecutor secureShellExecutor = new SecureShellExecutor(secureContext.getHost(),
        secureContext.getUsername(), secureContext.getPrivateKeyFile().getAbsolutePath());
    secureShellExecutor.execute(MAKE_DIRECTORIES_COMMAND + remoteDirectory);
  }
}
