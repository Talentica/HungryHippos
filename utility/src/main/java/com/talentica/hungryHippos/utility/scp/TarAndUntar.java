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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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
      tarOut.flush();
      in.close();
    }
    tarOut.close();
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
      dest.close();
    }
    tis.close();
  }
}
