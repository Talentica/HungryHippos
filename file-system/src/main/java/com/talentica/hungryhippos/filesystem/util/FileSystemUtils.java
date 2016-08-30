package com.talentica.hungryhippos.filesystem.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import com.google.common.base.Strings;
import com.talentica.hungryHippos.coordination.ZkUtils;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.utility.FileSystemConstants;

/**
 * Created by rajkishoreh on 8/7/16.
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
  public static void createDirectory(String dirName) {
    File directory = new File(dirName);
    if (!directory.exists()) {
      directory.mkdirs();
    }
    if (!directory.isDirectory()) {
      throw new RuntimeException(dirName + " is exists and is not a directory");
    }
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
      bis.close();
    }
    bos.close();
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
   * @param path
   * @param isFile
     */
  public static void validatePath(String path, boolean isFile) {
    if (Strings.isNullOrEmpty(path) || !path.startsWith(FileSystemConstants.ZK_PATH_SEPARATOR)
            ||path.contains("..")||path.contains("//")||path.contains("./")||path.contains("/.")) {
      throw new RuntimeException("Invalid path");
    }
    if (isFile) {
      if (path.endsWith(FileSystemConstants.ZK_PATH_SEPARATOR)) {
        throw new RuntimeException("Invalid file path");
      }
    } else {
      if (!path.endsWith(FileSystemConstants.ZK_PATH_SEPARATOR)) {
        throw new RuntimeException("Invalid directory path");
      }
    }
    checkSubPaths(path);

  }

  /**
   * Checks if a subpath is representing existing file
   * @param path
     */
  private static void checkSubPaths(String path){
    String[] strings =  path.split(FileSystemConstants.ZK_PATH_SEPARATOR);
    String fileSystemRootNode = CoordinationConfigUtil.getZkCoordinationConfigCache().getZookeeperDefaultConfig().getFilesystemPath();
    String relativeNodePath = "";
    for (int i = 1; i < strings.length-1; i++) {
      relativeNodePath = relativeNodePath+ FileSystemConstants.ZK_PATH_SEPARATOR +strings[i];
      String absoluteNodePath = fileSystemRootNode + relativeNodePath;
      if(ZkUtils.checkIfNodeExists(absoluteNodePath)){
        String nodeData = (String) ZkUtils.getNodeData(absoluteNodePath);
        if(FileSystemConstants.IS_A_FILE.equals(nodeData)){
          throw new RuntimeException("Invalid path : "+path+".\nFile with path : "+relativeNodePath+" already exists");
        }
      } else {
        break;
      }
    }
  }

  /**
   * Deletes the file or removes the folder recursively
   * @param file
   */
  public static void deleteFilesRecursively(File file){
    if(file.isDirectory()){
      String[] fileList=  file.list();
      for (int i = 0; i < fileList.length; i++) {
        deleteFilesRecursively(new File(fileList[i]));
      }
    }
    file.delete();
  }

  /**
   * Creates a new file in localPath
   * @param localPath
   * @return
   * @throws IOException
     */
  public static File createNewFile(String localPath) throws IOException {
    File file = new File(localPath);
    if(file.exists()){
      file.delete();
    }else{
      file.getParentFile().mkdirs();
    }
    file.createNewFile();
    return file;
  }


}
