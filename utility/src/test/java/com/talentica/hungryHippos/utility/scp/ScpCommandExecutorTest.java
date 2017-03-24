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

import org.junit.Ignore;
import org.junit.Test;

public class ScpCommandExecutorTest {

  private static final String userName = "root";
  private static final String host = "139.59.28.135";
  private static final String remoteDir = "test/sharding-files.tar.gz";
  private static final String localDir = "/home/sohanc/ScpAndGzip";
  
  @Test
  @Ignore
  public void testDownload(){
    ScpCommandExecutor.download(userName, host, remoteDir, localDir);
  }
  
  @Test
  @Ignore
  public void testUpload(){
    ScpCommandExecutor.upload(userName, host, remoteDir, localDir);
  }
}
