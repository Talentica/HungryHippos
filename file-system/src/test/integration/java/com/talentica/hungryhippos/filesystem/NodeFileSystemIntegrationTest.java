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
package com.talentica.hungryhippos.filesystem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.junit.Before;
import org.junit.Test;


public class NodeFileSystemIntegrationTest {


  String path = System.getProperty("user.home") + File.separatorChar + "hungryhippos";
  private NodeFileSystem nodeFileSystem = null;

  @Before
  public void setUp() {
    nodeFileSystem = new NodeFileSystem(path);
  }

  /**
   * Create a directory
   */
  @Test
  public void testCreateDir() {
    String loc = nodeFileSystem.createDir(path);
    assertEquals(loc, path);
  }

  /**
   * create a file inside the directory create above
   */
  @Test
  public void testCreateFile() {
    String fileName = "a.txt";
    String loc = nodeFileSystem.createFile(path + File.separatorChar + "a.txt");
    assertEquals(loc, path + File.separatorChar + fileName);
  }

  /**
   * check whether the directory exist.
   */
  @Test
  public void testCheckFileExistsInNode() {
    boolean exists = nodeFileSystem.checkFileExistsInNode(path);
    assertTrue(exists);
  }

  /**
   * A negative scenario, where the file is not present in system
   */
  @Test
  public void testCheckFileExistsInNodeNegative() {
    boolean exists = nodeFileSystem.checkFileExistsInNode(path + "th");
    assertFalse(exists);
  }

  /**
   * retrieve only regularFile path
   */
  @Test
  public void testGetAllFilesPath() {

    List<String> regularFiles =
        nodeFileSystem.getAllRgularFilesPath("/home/sudarshans/hungryhippos");
    assertNotEquals(regularFiles.size(), -1);
  }

  /**
   * retrieve only directory path
   */
  @Test
  public void testGetAllDirectoryPath() {

    List<String> regularFiles = nodeFileSystem.getAllDirectoryPath("/home/sudarshans/hungryhippos");
    assertNotEquals(regularFiles.size(), -1);
  }

  /**
   * Test should fail as the hungryhippos folder is not empty.
   */
  @Test
  public void testDeleteFile() {

    try {
      nodeFileSystem.deleteFile("/home/sudarshans/hungryhippos");
      assertFalse(true);
    } catch (RuntimeException e) {
      assertTrue(true);
    }
  }

  /**
   * Test will pass even when argument folder is not empty. it will remove all the subfolder of the
   * argument folder. Note:- It shouldn't deleteHungryHipposFileSystem.
   */
  @Test
  public void testDeleteAllFile() {

    try {
      nodeFileSystem.deleteAllFilesInsideAFolder("/home/sudarshans/hungryhippos");
      assertTrue(true);
    } catch (RuntimeException e) {
      assertTrue(false);
    }
  }

}
