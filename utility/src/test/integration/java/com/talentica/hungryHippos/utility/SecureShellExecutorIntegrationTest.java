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
package com.talentica.hungryHippos.utility;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class SecureShellExecutorIntegrationTest {

  private SecureShellExecutor secureShellExecutor = null;

  @Before
  public void setup() {
    secureShellExecutor = new SecureShellExecutor("107.170.3.50", "root", "~/.ssh/id_rsa");
  }

  @Test
  public void testExecute() {
    List<String> output = secureShellExecutor.execute("echo hello world");
    Assert.assertNotNull(output);
    Assert.assertEquals(1, output.size());
    Assert.assertEquals("hello world", output.get(0));
  }

  @Test
  public void testExecuteForRunningJavaCommand() {
    secureShellExecutor = new SecureShellExecutor("localhost", "nitink", "~/.ssh/id_rsa");
    List<String> output = secureShellExecutor.execute(
        "java -cp git/HungryHippos/installation/lib/test-jobs.jar com.talentica.hungryHippos.test.sum.SumJobMatrixImpl");
    Assert.assertNotNull(output);
    Assert.assertEquals(28, output.size());
    Assert.assertEquals("26", output.get(27));
  }

}
