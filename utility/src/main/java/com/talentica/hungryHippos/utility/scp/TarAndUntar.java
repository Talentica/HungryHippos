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
package com.talentica.hungryHippos.utility.scp;

import java.io.*;
import java.util.Map;
import java.util.Set;

import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarInputStream;
import org.kamranzafar.jtar.TarOutputStream;

public class TarAndUntar {

  public static void createTar(String sourceFolder, Set<String> fileNames, String destination)
      throws IOException {
    FileOutputStream out = new FileOutputStream(destination);
    TarOutputStream tarOut = new TarOutputStream(new BufferedOutputStream(out));
    for (String fileName : fileNames) {
      File file = new File(sourceFolder + File.separator + fileName);
      tarOut.putNextEntry(new TarEntry(file, file.getName()));
      BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
      int count;
      byte data[] = new byte[2048];
      while ((count = in.read(data)) != -1) {
        tarOut.write(data, 0, count);
      }
      in.close();
    }
    tarOut.flush();
    out.flush();
    tarOut.close();
    out.close();
  }
  
  public static void untar(String tarFile,String destinationFolder) throws IOException{
    TarInputStream tis = new TarInputStream(new BufferedInputStream(new FileInputStream(tarFile)));
    TarEntry entry;
    System.out.println(new File(destinationFolder).exists());
    if(! new File(destinationFolder).exists()){
      new File(destinationFolder).mkdirs();
    }
    while((entry = tis.getNextEntry()) != null){
      int count;
      byte data[] = new byte[2048];
      FileOutputStream fos = new FileOutputStream(destinationFolder + File.separator + entry.getName());
      BufferedOutputStream dest = new BufferedOutputStream(fos);
      while((count = tis.read(data)) != -1){
        dest.write(data, 0, count);
      }
      dest.flush();
      fos.flush();
      dest.close();
      fos.close();
    }
    tis.close();
  }


  public static void untarAndAppend(String tarFile, String destinationFolder) throws FileNotFoundException {
    TarInputStream tis = null;
    try {
      tis = new TarInputStream(new BufferedInputStream(new FileInputStream(tarFile)));
      TarEntry entry;
      System.out.println(new File(destinationFolder).exists());
      if(! new File(destinationFolder).exists()){
        new File(destinationFolder).mkdirs();
      }
      try {
        while((entry = tis.getNextEntry()) != null){
          int count;
          byte data[] = new byte[2048];
          FileOutputStream fos = new FileOutputStream(destinationFolder + File.separator + entry.getName(),true);
          BufferedOutputStream dest = new BufferedOutputStream(fos);
          while((count = tis.read(data)) != -1){
            dest.write(data, 0, count);
          }
          dest.flush();
          fos.flush();
          dest.close();
          fos.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    } catch (FileNotFoundException e) {
      throw e;
    }finally {
      try {
        tis.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }


  }

  public static void untarToStream(String tarFile, Map<String,? extends OutputStream> outputStreamMap) throws FileNotFoundException {
    TarInputStream tis = null;
    try {
      tis = new TarInputStream(new BufferedInputStream(new FileInputStream(tarFile)));
      TarEntry entry;
      try {
        while((entry = tis.getNextEntry()) != null) {
          if (entry.getSize() > 0) {
            int count;
            byte data[] = new byte[2048];
            OutputStream os = outputStreamMap.get(entry.getName());
            if (os instanceof BufferedOutputStream) {
              BufferedOutputStream dest = (BufferedOutputStream) os;
              while ((count = tis.read(data)) != -1) {
                dest.write(data, 0, count);
              }
              dest.flush();
            } else if (os instanceof FileOutputStream) {
              FileOutputStream dest = (FileOutputStream) os;
              while ((count = tis.read(data)) != -1) {
                dest.write(data, 0, count);
              }
              dest.flush();
            } else {
              while ((count = tis.read(data)) != -1) {
                os.write(data, 0, count);
              }
              os.flush();
            }

          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    } catch (FileNotFoundException e) {
      throw e;
    }finally {
      try {
        tis.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }


  }
}
