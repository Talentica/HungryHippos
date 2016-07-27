package com.talentica.hungryhippos.filesystem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

import org.junit.Test;


public class NodeFileSystemTest {


  String path = System.getProperty("user.home") + File.separatorChar + "hungryhippos";

  /**
   * Create a directory
   */
  @Test
  public void testCreateDir() {
    String loc = NodeFileSystem.createDir(path);
    assertEquals(loc, path);
  }

  /**
   * create a file inside the directory create above
   */
  @Test
  public void testCreateFile() {
    String fileName = "a.txt";
    String loc = NodeFileSystem.createFile(path + File.separatorChar + "a.txt");
    assertEquals(loc, path + File.separatorChar + fileName);
  }

  /**
   * check whether the directory exist.
   */
  @Test
  public void testCheckFileExistsInNode() {
    boolean exists = NodeFileSystem.checkFileExistsInNode(path);
    assertTrue(exists);
  }

  /**
   * A negative scenario, where the file is not present in system
   */
  @Test
  public void testCheckFileExistsInNodeNegative() {
    boolean exists = NodeFileSystem.checkFileExistsInNode(path + "th");
    assertFalse(exists);
  }

  /**
   * retrieve only regularFile path
   */
  @Test
  public void testGetAllFilesPath() {

    List<String> regularFiles =
        NodeFileSystem.getAllRgularFilesPath("/home/sudarshans/hungryhippos");
    assertNotEquals(regularFiles.size(), -1);
  }

  /**
   * retrieve only directory path
   */
  @Test
  public void testGetAllDirectoryPath() {

    List<String> regularFiles = NodeFileSystem.getAllDirectoryPath("/home/sudarshans/hungryhippos");
    assertNotEquals(regularFiles.size(), -1);
  }

  /**
   * Test should fail as the hungryhippos folder is not empty.
   */
  @Test
  public void testDeleteFile() {

    try {
      NodeFileSystem.deleteFile("/home/sudarshans/hungryhippos");
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
      NodeFileSystem.deleteAllFilesInsideAFolder("/home/sudarshans/hungryhippos");
      assertTrue(true);
    } catch (RuntimeException e) {
      assertTrue(false);
    }
  }

}
