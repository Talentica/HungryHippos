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
package com.talentica.hungryhippos.filesystem.command;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.talentica.hungryhippos.filesystem.Exception.HungryHipposFileSystemException;


@Ignore
public class GetCommandTest {

  String folder = "/sudarshans/input";
  String command = "get";

  String clientConfig =
      "//home//sudarshans//RD//HH_NEW//HungryHippos//configuration-schema//src//main//resources//distribution//client-config.xml";


  @Before
  public void setUp() throws HungryHipposFileSystemException {
 

  }

  @Test
  public void testExecute_get() {
    String option_s = "-s";
    CommandLineParser parser = new DefaultParser();
    String[] args = {command, folder, option_s};
    GetCommand.execute(parser, args);
  }

}
