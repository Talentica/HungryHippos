package com.talentica.hungryhippos.filesystem.main;

import java.io.File;

import com.talentica.hungryhippos.filesystem.CleanFileSystem;

/**
 * 
 * @author sudarshans
 *
 */
public class CleanFileSystemMain {

  private static final String ROOT_DIR = "HungryHipposFs";

  /**
   * 
   * @param args
   */
  public static void main(String[] args) {

    CleanFileSystem.DeleteFilesWhichAreNotPartOFZK(
        System.getProperty("user.dir") + File.separatorChar + ROOT_DIR);
  }

}
