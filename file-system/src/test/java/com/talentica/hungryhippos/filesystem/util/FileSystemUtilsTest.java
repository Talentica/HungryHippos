package com.talentica.hungryhippos.filesystem.util;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;

/**
 * This is a Test class for FileSystemUtils. Created by rajkishoreh on 11/7/16.
 */
@Ignore
public class FileSystemUtilsTest {

  public String fileSystemRootNode;
  private HungryHippoCurator curator;

  @Before
  public void setup() throws FileNotFoundException, JAXBException {
    curator = curator.getInstance("localhost:2181");

    fileSystemRootNode = CoordinationConfigUtil.getZkCoordinationConfigCache()
        .getZookeeperDefaultConfig().getFilesystemPath();
  }

  @Test
  public void testGetDimensionOperand() {
    int dimensionOperand = FileSystemUtils.getDimensionOperand(1);
    assertEquals(dimensionOperand, 1);
    dimensionOperand = FileSystemUtils.getDimensionOperand(3);
    assertEquals(dimensionOperand, 4);
  }

  @Test
  public void testCreateDirectory() {
    String dirName = "TestDir";
    File directory = new File(dirName);
    FileSystemUtils.createDirectory(dirName);
    directory.deleteOnExit();
    assertTrue(directory.exists() && directory.isDirectory());
  }

  @Test
  public void testValidatePathIsAFile() {
    String filePath = "/dir1/dir2/file1.txt";
    FileSystemUtils.validatePath(filePath, true);
    Assert.assertTrue(true);
  }

  @Test
  public void testValidatePathIsADirectory() {
    String filePath = "/dir1/dir2/";
    FileSystemUtils.validatePath(filePath, false);
    Assert.assertTrue(true);
  }

  @Test
  public void testValidatePath() throws HungryHippoException {
    String filePath = "/dir1/dir2/file1.txt";
    FileSystemUtils.validatePath(filePath, true);
    curator.createPersistentNode(fileSystemRootNode + filePath);
    FileSystemUtils.validatePath(filePath, true);
    String filePath1 = "/dir1/dir2/";
    FileSystemUtils.validatePath(filePath1, false);
    String filePath2 = "/dir1/dir2/file2.txt";
    FileSystemUtils.validatePath(filePath2, true);

    try {
      String filePath3 = "/dir1/dir2/file1.txt/dir3/file2.txt";
      FileSystemUtils.validatePath(filePath3, true);
      Assert.assertTrue(false);
    } catch (RuntimeException e) {
      e.printStackTrace();
      Assert.assertTrue(true);
    }


  }

  @Test
  public void testValidatePathInValid() {
    try {
      String filePath = "../dir1/dir2/text";
      FileSystemUtils.validatePath(filePath, true);
      Assert.assertTrue(false);
    } catch (RuntimeException e) {
      e.printStackTrace();
      Assert.assertTrue(true);
    }

    try {
      String filePath = "/dir1/dir2/../text";
      FileSystemUtils.validatePath(filePath, true);
      Assert.assertTrue(false);
    } catch (RuntimeException e) {
      e.printStackTrace();
      Assert.assertTrue(true);
    }

    try {
      String filePath = "/dir1/dir2/./text";
      FileSystemUtils.validatePath(filePath, true);
      Assert.assertTrue(false);
    } catch (RuntimeException e) {
      e.printStackTrace();
      Assert.assertTrue(true);
    }

    try {
      String filePath = "./dir1/dir2/text";
      FileSystemUtils.validatePath(filePath, true);
      Assert.assertTrue(false);
    } catch (RuntimeException e) {
      e.printStackTrace();
      Assert.assertTrue(true);
    }

    try {
      String filePath = "/dir1/dir2/text/.";
      FileSystemUtils.validatePath(filePath, true);
      Assert.assertTrue(false);
    } catch (RuntimeException e) {
      e.printStackTrace();
      Assert.assertTrue(true);
    }

    try {
      String filePath = "/dir1/dir2/.text";
      FileSystemUtils.validatePath(filePath, true);
      Assert.assertTrue(false);
    } catch (RuntimeException e) {
      e.printStackTrace();
      Assert.assertTrue(true);
    }

    try {
      String filePath = "/dir1/dir2/ text";
      FileSystemUtils.validatePath(filePath, true);
      Assert.assertTrue(false);
    } catch (RuntimeException e) {
      e.printStackTrace();
      Assert.assertTrue(true);
    }

    try {
      String filePath = "/dir1/dir2/#text";
      FileSystemUtils.validatePath(filePath, true);
      Assert.assertTrue(false);
    } catch (RuntimeException e) {
      e.printStackTrace();
      Assert.assertTrue(true);
    }

    try {
      String filePath = "/dir1 /dir2/#text";
      FileSystemUtils.validatePath(filePath, true);
      Assert.assertTrue(false);
    } catch (RuntimeException e) {
      e.printStackTrace();
      Assert.assertTrue(true);
    }
  }
}
