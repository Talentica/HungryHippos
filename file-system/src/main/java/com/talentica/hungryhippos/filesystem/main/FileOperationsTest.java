package com.talentica.hungryhippos.filesystem.main;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import org.junit.Test;

public class FileOperationsTest {


  private static final String HUNGRYHIPPOS = "HungryHippos";

  @Test
  public void testGetDefaultFileSystem() {
    FileSystem fs = FileOperations.getDefaultFileSystem();
    assertNotNull(fs);
  }

  @Test
  public void testGetRoot() {
    String os = System.getProperty("os.name");
    String root = FileOperations.getRoot();
    assertNotNull(root);
    if (os.equalsIgnoreCase("Linux")) {
      assertEquals("/", root);
    }

  }

  @Test
  public void testGetHungryHipposRoot() {
    String root = FileOperations.getHungryHipposRoot();
    assertNotNull(root);
    assertTrue(root.contains(HUNGRYHIPPOS));
  }

  @Test
  public void testGetUserHome() {
    String userHome = FileOperations.getUserHome();
    assertNotNull(userHome);
  }

  @Test
  public void testGetUserDir() {
    String userDir = FileOperations.getUserHome();
    assertNotNull(userDir);
  }

  @Test
  public void testCreatePath() {
    String name = "test";
    Path path = FileOperations.createPath(name);
    assertNotNull(path);
    assertNotEquals(name, path);
  }

  @Test
  public void testSetPermission() {
    String permission = "rwx--x--x";
    Set<PosixFilePermission> set = FileOperations.setPermission(permission);
    assertNotNull(set);
    assertNotEquals(0, set.size());
  }

  /**
   * This test is to whether the method is throwing an exception when illegal argument is provided. 
   */
  @Test
  public void testSetPermissionFail() {
    try{
    String permission = "rwx--x--";
    Set<PosixFilePermission> set = FileOperations.setPermission(permission);
    }catch(Exception e){
     assertTrue(true);
   }
    assertTrue(false);
  }

  @Test
  public void testSetAttributes() {
    fail("Not yet implemented");
  }



  @Test
  public void testCreateDirectory() {
    fail("Not yet implemented");
  }

  @Test
  public void testDeleteFileString() {
    fail("Not yet implemented");
  }

  @Test
  public void testDeleteFilePath() {
    fail("Not yet implemented");
  }

  @Test
  public void testDeleteEverything() {
    fail("Not yet implemented");
  }

  @Test
  public void testListFilesInsideHHRoot() {
    fail("Not yet implemented");
  }

  @Test
  public void testListFilesString() {
    fail("Not yet implemented");
  }

  @Test
  public void testListFilesPath() {
    fail("Not yet implemented");
  }

  @Test
  public void testCreateFileStringFileAttributeOfSetOfPosixFilePermission() {
    fail("Not yet implemented");
  }

  @Test
  public void testCreateFilePathFileAttributeOfSetOfPosixFilePermission() {
    fail("Not yet implemented");
  }

  @Test
  public void testCheckFileExistStringLinkOptionArray() {
    fail("Not yet implemented");
  }

  @Test
  public void testCheckFileExistPathLinkOptionArray() {
    fail("Not yet implemented");
  }

  @Test
  public void testIsRegularExecutableFile() {
    fail("Not yet implemented");
  }

  @Test
  public void testIsReadableString() {
    fail("Not yet implemented");
  }

  @Test
  public void testIsWritableString() {
    fail("Not yet implemented");
  }

  @Test
  public void testIsExecutableString() {
    fail("Not yet implemented");
  }

  @Test
  public void testIsReadablePath() {
    fail("Not yet implemented");
  }

  @Test
  public void testIsWritablePath() {
    fail("Not yet implemented");
  }

  @Test
  public void testIsExecutablePath() {
    fail("Not yet implemented");
  }

  @Test
  public void testSize() {
    fail("Not yet implemented");
  }

  @Test
  public void testIsDirectory() {
    fail("Not yet implemented");
  }

  @Test
  public void testIsRegularFile() {
    fail("Not yet implemented");
  }

  @Test
  public void testIsSymbolicLink() {
    fail("Not yet implemented");
  }

  @Test
  public void testIsHidden() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetLastModifiedTime() {
    fail("Not yet implemented");
  }

  @Test
  public void testSetLastModifiedTime() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetOwner() {
    fail("Not yet implemented");
  }

  @Test
  public void testSetOwner() {
    fail("Not yet implemented");
  }

  @Test
  public void testGetAttribute() {
    fail("Not yet implemented");
  }

}
