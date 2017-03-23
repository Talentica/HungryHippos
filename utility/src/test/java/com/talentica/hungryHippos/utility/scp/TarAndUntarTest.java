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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class TarAndUntarTest {

  private final String sourceFolder = "/home/sohanc/D_drive/dataSet";
  private final String destination = "/home/sohanc/D_drive/dataSet/result.tar";
  private final String tarFile = "/home/sohanc/D_drive/dataSet/result.tar";
  private String destinationFolder = "/home/sohanc/D_drive/untarFolder";
  private Set<String> fileNames;

  @Before
  public void setUp() {
    fileNames = new HashSet<String>();
    fileNames.add("e.txt");
    fileNames.add("p.txt");
  }

  @Test
  @Ignore
  public void createTarTest() throws IOException {
    TarAndUntar.createTar(sourceFolder, fileNames, destination);
  }

  @Test

  public void untarTest() throws IOException {
    TarAndUntar.untar(tarFile, destinationFolder);
  }
}
