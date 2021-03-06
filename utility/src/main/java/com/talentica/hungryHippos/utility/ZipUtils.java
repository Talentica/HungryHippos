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

import java.io.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * This class contains utilities for handling zip files.
 *
 * Created by rajkishoreh on 28/6/16.
 */
public class ZipUtils {

  /**
   * This method unzips the zipped file and stores in the destination Directory
   * 
   * @param fileToBeUnzipped
   * @param destinationDirectory
   */
  public static void unZipFile(String fileToBeUnzipped, String destinationDirectory) {
    ZipFile zip = null;
    try {
      int BUFFER = 1024;
      byte[] buffer = new byte[BUFFER];
      zip = new ZipFile(new File(fileToBeUnzipped));
      Enumeration zipEntries = zip.entries();

      String zipFileName = zip.getName().substring(0, zip.getName().lastIndexOf("."));
      destinationDirectory = destinationDirectory + File.separator + zipFileName;
      new File(destinationDirectory).mkdir();

      // loop iterates for multiple zipentries and creates files and directories
      while (zipEntries.hasMoreElements()) {
        ZipEntry zipEntry = (ZipEntry) zipEntries.nextElement();
        String zipEntryName = zipEntry.getName();
        File newFile = new File(destinationDirectory, zipEntryName);
        newFile.getParentFile().mkdirs();
        // creates files
        if (!zipEntry.isDirectory()) {

          BufferedInputStream bis = new BufferedInputStream(zip.getInputStream(zipEntry));
          int len;

          FileOutputStream fos = new FileOutputStream(newFile);
          BufferedOutputStream bos = new BufferedOutputStream(fos, BUFFER);


          while ((len = bis.read(buffer)) != -1) {
            bos.write(buffer, 0, len);
          }
          bos.flush();
          fos.flush();
          bos.close();
          bis.close();
          fos.close();
        } else {
          // creates blank directories if present
          newFile.mkdir();
        }


      }


    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (zip != null) {
          zip.close();
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }


  }

  /**
   * This method zips the folder and stores it in destinationZip
   * 
   * @param folderToBeZipped
   * @param destinationZip
   */
  public static void zipFolder(String folderToBeZipped, String destinationZip) {
    try {
      int BUFFER = 1024;
      List<File> fileList = new ArrayList<>();
      byte[] buffer = new byte[BUFFER];
      FileOutputStream fos = new FileOutputStream(destinationZip);
      ZipOutputStream zos = new ZipOutputStream(fos);
      File inputFolder = new File(folderToBeZipped);

      // populates the fileList from the inputFolder
      generateFileList(inputFolder, fileList);

      for (File file : fileList) {
        // adds files
        if (!file.isDirectory()) {
          FileInputStream fis = new FileInputStream(file);
          BufferedInputStream bis = new BufferedInputStream(fis, BUFFER);
          ZipEntry ze = new ZipEntry(file.getCanonicalPath().substring(
              inputFolder.getCanonicalPath().length() + 1, file.getCanonicalPath().length()));
          zos.putNextEntry(ze);
          int len;
          while ((len = bis.read(buffer)) != -1) {
            zos.write(buffer, 0, len);
          }
          zos.flush();
          fos.flush();
          zos.closeEntry();
          bis.close();
        } else {
          // adds blank directories if present
          ZipEntry ze = new ZipEntry(file.getCanonicalPath().substring(
              inputFolder.getCanonicalPath().length() + 1, file.getCanonicalPath().length()) + "/");
          zos.putNextEntry(ze);
          zos.closeEntry();
        }


      }
      zos.close();
      fos.close();

    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  /**
   * This method adds the child directories and files in the fileList
   * 
   * @param dir
   * @param fileList
   */
  private static void generateFileList(File dir, List<File> fileList) {
    File[] files = dir.listFiles();
    if(files!=null) {
      for (File file : files) {
        fileList.add(file);
        if (file.isDirectory()) {
          generateFileList(file, fileList);
        }

      }
    }else{
      throw new RuntimeException(dir.getAbsolutePath()+" not a valid path");
    }
  }

}
