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
package com.talentica.hungryHippos.utility.scp;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.jcraft.jsch.JSchException;

public class JscpIntegrationTest {

  private static final String SOURCE_FOLDER = "/home/nitink/hungryhippos";

  private static final String DESTINATION_FOLDER = "/root";

  private SecureContext context;

  @Before
  public void setup() {
    context = new SecureContext("root", "138.68.27.207");
    context.setTrustAllHosts(true);
    context.setPrivateKeyFile(
        new File(getClass().getClassLoader().getResource("new_scp_test_key").getFile()));
  }

  @Test
  public void testExec() throws IOException, JSchException {
    Jscp.scpTarGzippedFile(context, SOURCE_FOLDER, DESTINATION_FOLDER, "test-copy");
  }

}
