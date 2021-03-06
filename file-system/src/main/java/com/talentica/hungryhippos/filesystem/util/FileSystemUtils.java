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
package com.talentica.hungryhippos.filesystem.util;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.exception.HungryHippoException;
import com.talentica.hungryHippos.utility.FileSystemConstants;

import java.io.*;
import java.util.List;

/**
 * {@code FileSystemUtils} has utility methods for handling fileSystem related operations.
 * 
 * @author rajkishoreh
 * @since 8/7/16.
 */
public class FileSystemUtils {

  /**
   * This method returns the equivalent dimension Operand
   *
   * @param dimension
   * @return
   */
  public static int getDimensionOperand(int dimension) {
    if (dimension < 0) {
      throw new RuntimeException("Invalid Dimension : " + dimension);
    }
    // Determines the dataFileNodes from which the data has to be retrieved.
    int dimensionOperand = 1;
    if (dimension > 0) {
      dimensionOperand = 1 << (dimension - 1);
    } else {
      // if dimension specified is zero , then all the dataFileNodes are selected.
      dimensionOperand = 0;
    }
    return dimensionOperand;
  }

  /**
   * This method creates a directory if it doesn't exist
   *
   * @param dirName
   */
  public static boolean createDirectory(String dirName) {
    boolean flag = false;
    File directory = new File(dirName);
    if (!directory.exists()) {
      flag = directory.mkdirs();
    }
    if (!directory.isDirectory()) {
      throw new RuntimeException(dirName + " is exists and is not a directory");
    }
    return flag;
  }

  /**
   * Combines multiple files into a single file
   * 
   * @param fileNames
   * @param destFile
   * @throws IOException
   */
  public static void combineFiles(List<String> fileNames, String destFile) throws IOException {

    for (String fileName : fileNames) {
      File file = new File(fileName);
      if (!(file.exists() && file.isFile())) {
        throw new RuntimeException(fileName + " is not a file");
      }
    } ;
    FileOutputStream fos = new FileOutputStream(destFile);
    BufferedOutputStream bos = new BufferedOutputStream(fos);
    int len;
    FileInputStream fis;
    BufferedInputStream bis;
    byte[] buffer = new byte[2048];
    for (String fileName : fileNames) {
      fis = new FileInputStream(fileName);
      bis = new BufferedInputStream(fis);
      while ((len = bis.read(buffer)) > -1) {
        bos.write(buffer, 0, len);;
      }
      bos.flush();
      fos.flush();
      bis.close();
    }
    bos.close();
    fos.close();
  }

  /**
   * Deletes a list of files
   * 
   * @param fileNames
   */
  public static void deleteFiles(List<String> fileNames) {
    for (String fileName : fileNames) {
      new File(fileName).delete();
    }
  }

  /**
   * Checks if the path is valid
   * 
   * @param path
   * @param isFile
   */
  public static void validatePath(String path, boolean isFile) {
    String validPattern = "^[0-9A-Za-z./-]*";
    if (path==null||"".equals(path) || !path.startsWith(FileSystemConstants.ZK_PATH_SEPARATOR)
        || path.contains("..") || path.contains("//") || path.contains("./") || path.contains("/.")
        || !path.matches(validPattern)) {
      throw new RuntimeException("Invalid path :" + path);
    }
    if (isFile) {
      if (path.endsWith(FileSystemConstants.ZK_PATH_SEPARATOR)) {
        throw new RuntimeException("Invalid file path :" + path);
      }
    } else {
      if (!path.endsWith(FileSystemConstants.ZK_PATH_SEPARATOR)) {
        throw new RuntimeException("Invalid directory path :" + path);
      }
    }
    checkSubPaths(path);

  }

  /**
   * Checks if a subpath is representing existing file
   * 
   * @param path
   */
  private static void checkSubPaths(String path) {
    String[] strings = path.split(FileSystemConstants.ZK_PATH_SEPARATOR);
    String fileSystemRootNode = CoordinationConfigUtil.getFileSystemPath();
    String relativeNodePath = "";
    HungryHippoCurator curator = HungryHippoCurator.getInstance();
    for (int i = 1; i < strings.length - 1; i++) {
      relativeNodePath = relativeNodePath + FileSystemConstants.ZK_PATH_SEPARATOR + strings[i];
      String absoluteNodePath = fileSystemRootNode + relativeNodePath;
      try {
        if (curator.checkExists(absoluteNodePath)) {
          String nodeData = (String) curator.getZnodeData(absoluteNodePath);
          if (FileSystemConstants.IS_A_FILE.equals(nodeData)) {
            throw new RuntimeException("Invalid path : " + path + ".\nFile with path : "
                + relativeNodePath + " already exists");
          }
        } else {
          break;
        }
      } catch (HungryHippoException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  /**
   * Deletes the file or removes the folder recursively
   * 
   * @param file
   */
  public static void deleteFilesRecursively(File file) {
    if (file.isDirectory()) {
      String[] fileList = file.list();
      if (fileList != null) {
        for (int i = 0; i < fileList.length; i++) {
          deleteFilesRecursively(new File(fileList[i]));
        }
      }
    }
    file.delete();
  }

  /**
   * Creates a new file in localPath
   * 
   * @param localPath
   * @return
   * @throws IOException
   */
  public static File createNewFile(String localPath) throws IOException {
    File file = new File(localPath);
    if (file.exists()) {
      file.delete();
    } else {
      file.getParentFile().mkdirs();
    }
    file.createNewFile();
    return file;
  }


}
